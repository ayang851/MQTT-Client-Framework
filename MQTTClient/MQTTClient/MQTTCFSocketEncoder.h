//
// MQTTCFSocketEncoder.h
// MQTTClient.framework
//
// Copyright © 2013-2017, Christoph Krey. All rights reserved.
//

#import <Foundation/Foundation.h>

typedef NS_ENUM(NSInteger, MQTTCFSocketEncoderState) {
    MQTTCFSocketEncoderStateInitializing,
    MQTTCFSocketEncoderStateReady,
    MQTTCFSocketEncoderStateError
};

@class MQTTCFSocketEncoder;

@protocol MQTTCFSocketEncoderDelegate <NSObject>

- (void)encoderDidOpen:(MQTTCFSocketEncoder *)sender;
- (void)encoder:(MQTTCFSocketEncoder *)sender didFailWithError:(NSError *)error;
- (void)encoderdidClose:(MQTTCFSocketEncoder *)sender;

@end

@interface MQTTCFSocketEncoder : NSObject <NSStreamDelegate>

@property (nonatomic) MQTTCFSocketEncoderState state;
@property (strong, nonatomic) NSError *error;
@property (strong, nonatomic) NSOutputStream *stream;
@property (weak, nonatomic ) id<MQTTCFSocketEncoderDelegate> delegate;

- (void)open;
- (void)close;

/** Appends data to the outgoing buffer and writes as much as the stream accepts right now.
 *  Bytes that cannot be written stay buffered and are flushed on NSStreamEventHasSpaceAvailable,
 *  so this method never blocks on the socket, even for very large payloads.
 *  @param data bytes to send, or nil to flush already buffered bytes
 *  @return YES if the data was written or queued, NO if the encoder is not ready or the stream failed
 */
- (BOOL)send:(NSData *)data;

@end

