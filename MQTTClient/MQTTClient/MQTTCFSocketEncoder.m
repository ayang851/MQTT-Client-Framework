//
// MQTTCFSocketEncoder.m
// MQTTClient.framework
//
// Copyright © 2013-2017, Christoph Krey. All rights reserved.
//

#import "MQTTCFSocketEncoder.h"

#import "MQTTLog.h"

// Compact the pending buffer once the already-written prefix exceeds both a minimum
// absolute threshold and a fraction of the current buffer. This avoids a memmove on
// every partial write while not letting the prefix grow without bound for huge payloads.
static const NSUInteger MQTTCFSocketEncoderCompactMinThreshold = 64 * 1024;
static const CGFloat MQTTCFSocketEncoderCompactFraction = 0.25;

// Bound each call so the encoder never monopolizes its queue (often the main queue)
// while still draining eagerly. When the per-call budget is exhausted, a flush is
// scheduled asynchronously on the same queue to continue without blocking the caller.
static const NSUInteger MQTTCFSocketEncoderMaxWriteChunk = 64 * 1024;
static const NSUInteger MQTTCFSocketEncoderMaxWritePerCall = 256 * 1024;

@interface MQTTCFSocketEncoder()

@property (strong, nonatomic) NSMutableData *buffer;
@property (nonatomic) NSUInteger offset;
@property (strong, nonatomic) dispatch_queue_t queue;

@end

@implementation MQTTCFSocketEncoder

- (instancetype)init {
    self = [super init];
    self.state = MQTTCFSocketEncoderStateInitializing;
    self.buffer = [[NSMutableData alloc] init];
    self.stream = nil;
    return self;
}

- (void)dealloc {
    [self close];
}

- (void)open {
    (self.stream).delegate = self;
    [self.stream open];
}

- (void)close {
    [self.stream close];
    [self.stream setDelegate:nil];
    [self.buffer setLength:0];
    self.offset = 0;
}

- (void)setState:(MQTTCFSocketEncoderState)state {
    DDLogVerbose(@"[MQTTCFSocketEncoder] setState %ld/%ld", (long)_state, (long)state);
    _state = state;
}

- (void)stream:(NSStream *)sender handleEvent:(NSStreamEvent)eventCode {
    if (eventCode & NSStreamEventOpenCompleted) {
        DDLogVerbose(@"[MQTTCFSocketEncoder] NSStreamEventOpenCompleted");
    }
    if (eventCode & NSStreamEventHasBytesAvailable) {
        DDLogVerbose(@"[MQTTCFSocketEncoder] NSStreamEventHasBytesAvailable");
    }
    if (eventCode & NSStreamEventHasSpaceAvailable) {
        DDLogVerbose(@"[MQTTCFSocketEncoder] NSStreamEventHasSpaceAvailable");
        if (self.state == MQTTCFSocketEncoderStateInitializing) {
            self.state = MQTTCFSocketEncoderStateReady;
            [self.delegate encoderDidOpen:self];
        }
        
        if (self.state == MQTTCFSocketEncoderStateReady) {
            if (self.buffer.length) {
                [self send:nil];
            }
        }
    }
    if (eventCode & NSStreamEventEndEncountered) {
        DDLogVerbose(@"[MQTTCFSocketEncoder] NSStreamEventEndEncountered");
        self.state = MQTTCFSocketEncoderStateInitializing;
        self.error = nil;
        [self.buffer setLength:0];
        self.offset = 0;
        [self.delegate encoderdidClose:self];
    }
    if (eventCode & NSStreamEventErrorOccurred) {
        DDLogVerbose(@"[MQTTCFSocketEncoder] NSStreamEventErrorOccurred");
        self.state = MQTTCFSocketEncoderStateError;
        self.error = self.stream.streamError;
        [self.buffer setLength:0];
        self.offset = 0;
        [self.delegate encoder:self didFailWithError:self.error];
    }
}

- (BOOL)send:(NSData *)data {
    @synchronized(self) {
        if (self.state != MQTTCFSocketEncoderStateReady) {
            DDLogInfo(@"[MQTTCFSocketEncoder] not MQTTCFSocketEncoderStateReady");
            return NO;
        }
        
        if (data) {
            [self.buffer appendData:data];
        }
        
        return [self writeBuffer];
    }
}

// Writes the pending buffer, but only as much as the stream accepts right now.
// If the socket has no space available (a large payload on a slow link, for example) the
// remaining bytes stay buffered and are flushed from the NSStreamEventHasSpaceAvailable
// callback in -stream:handleEvent: - so no caller ever blocks on the socket.
- (BOOL)writeBuffer {
    if (!self.buffer.length) {
        return YES;
    }
    
    NSUInteger pendingTotal = self.buffer.length - self.offset;
    DDLogVerbose(@"[MQTTCFSocketEncoder] buffer to write (%lu)=%@...",
                 (unsigned long)pendingTotal,
                 [self.buffer subdataWithRange:NSMakeRange(self.offset, MIN(256, pendingTotal))]);
    
    NSUInteger writtenThisCall = 0;
    while (self.offset < self.buffer.length) {
        if (!self.stream.hasSpaceAvailable) {
            DDLogVerbose(@"[MQTTCFSocketEncoder] no space available, %lu bytes stay buffered",
                         (unsigned long)(self.buffer.length - self.offset));
            return YES;
        }
        
        if (writtenThisCall >= MQTTCFSocketEncoderMaxWritePerCall) {
            DDLogVerbose(@"[MQTTCFSocketEncoder] wrote %lu bytes this call, scheduling async flush",
                         (unsigned long)writtenThisCall);
            [self scheduleFlush];
            return YES;
        }
        
        NSUInteger pending = self.buffer.length - self.offset;
        NSUInteger chunk = MIN(pending, MQTTCFSocketEncoderMaxWriteChunk);
        NSInteger n = [self.stream write:(const uint8_t *)self.buffer.bytes + self.offset maxLength:chunk];
        
        if (n < 0) {
            DDLogVerbose(@"[MQTTCFSocketEncoder] streamError: %@", self.error);
            self.state = MQTTCFSocketEncoderStateError;
            self.error = self.stream.streamError;
            return NO;
        }
        
        if (n == 0) {
            DDLogVerbose(@"[MQTTCFSocketEncoder] stream accepted 0 bytes, waiting for next space event");
            return YES;
        }
        
        self.offset += n;
        writtenThisCall += n;
        
        if (n < chunk) {
            DDLogVerbose(@"[MQTTCFSocketEncoder] buffer partially written: %ld, %lu still pending",
                         (long)n,
                         (unsigned long)(self.buffer.length - self.offset));
        }
        
        if (self.offset >= self.buffer.length) {
            [self.buffer setLength:0];
            self.offset = 0;
        } else if (self.offset >= MAX(MQTTCFSocketEncoderCompactMinThreshold,
                                      (NSUInteger)(self.buffer.length * MQTTCFSocketEncoderCompactFraction))) {
            [self.buffer replaceBytesInRange:NSMakeRange(0, self.offset) withBytes:NULL length:0];
            self.offset = 0;
        }
    }
    
    return YES;
}

- (void)scheduleFlush {
    if (!self.queue) {
        return;
    }
    dispatch_queue_t queue = self.queue;
    __weak typeof(self) weakSelf = self;
    dispatch_async(queue, ^{
        __strong typeof(weakSelf) strongSelf = weakSelf;
        [strongSelf send:nil];
    });
}

@end
