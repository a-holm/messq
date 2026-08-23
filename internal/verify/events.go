// SPDX-License-Identifier: Apache-2.0

package verify

// eventName is the closed S2.4 event vocabulary as a typed enum. The fold switches over it,
// and the `exhaustive` linter (default-signifies-exhaustive: false) makes adding an event in
// a later issue a compile error until its fold arm exists. The values are the exact wire
// spellings the events table stores.
type eventName string

const (
	evServerStart       eventName = "server.start"
	evServerStop        eventName = "server.stop"
	evServerReload      eventName = "server.reload"
	evRecoveryUnclean   eventName = "recovery.unclean"
	evRecoveryReclaimed eventName = "recovery.reclaimed"
	evStorageFatal      eventName = "storage.fatal"
	evStreamCreate      eventName = "stream.create"
	evStreamUpdate      eventName = "stream.update"
	evStreamDelete      eventName = "stream.delete"
	evStreamPurge       eventName = "stream.purge"
	evRetentionExpire   eventName = "retention.expire"
	evRetentionBlocked  eventName = "retention.blocked"
	evConsumerCreate    eventName = "consumer.create"
	evConsumerUpdate    eventName = "consumer.update"
	evConsumerDelete    eventName = "consumer.delete"
	evConsumerSeek      eventName = "consumer.seek"
	evConsumerPause     eventName = "consumer.pause"
	evConsumerLag       eventName = "consumer.lag"
	evMsgPublish        eventName = "msg.publish"
	evMsgDup            eventName = "msg.dup"
	evMsgDeliver        eventName = "msg.deliver"
	evMsgAck            eventName = "msg.ack"
	evMsgAckDup         eventName = "msg.ack_dup"
	evMsgAckStale       eventName = "msg.ack_stale"
	evMsgNak            eventName = "msg.nak"
	evMsgTerm           eventName = "msg.term"
	evMsgExtend         eventName = "msg.extend"
	evMsgTimeout        eventName = "msg.timeout"
	evMsgDead           eventName = "msg.dead"
	evDLQRedrive        eventName = "dlq.redrive"
	evFlowBlocked       eventName = "flow.blocked"
	evDiskDegraded      eventName = "disk.degraded"
	evAuthDenied        eventName = "auth.denied"
	evAPIError          eventName = "api.error"
	evAdminAction       eventName = "admin.action"
)
