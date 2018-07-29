// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

import * as debugModule from "debug";
import { MessagingError } from "./amqp-common";
import { ConnectionContext } from "./connectionContext";
import { ReceiveMode, ReceiveOptions, OnError, OnMessage } from ".";
import { StreamingReceiver, ReceiveHandler, MessageHandlerOptions } from "./streamingReceiver";
import { BatchingReceiver } from "./batchingReceiver";
import { Message } from "./message";
import { Client } from "./client";

const debug = debugModule("azure:service-bus:subscription-client");

/**
 * Describes the options that can be provided while creating the SubscriptionClient.
 * @interface SubscriptionClientOptions
 */
export interface SubscriptionClientOptions {
  /**
   * @property {number} [receiveMode] The mode in which messages should be received.
   * Default: ReceiveMode.peekLock
   */
  receiveMode?: ReceiveMode;
  /**
   * @property {number} [maxConcurrentCalls] he maximum number of messages that should be
   * processed concurrently while in peek lock mode. Once this limit has been reached, more
   * messages will not be received until messages currently being processed have been settled.
   * Default: 1
   */
  maxConcurrentCalls?: number;
}

export class SubscriptionClient extends Client {
  /**
   * @property {string} topicPath The topic path.
   */
  topicPath: string;
  /**
   * @property {string} subscriptionName The subscription name.
   */
  subscriptionName: string;
  /**
   * @property {number} receiveMode The mode in which messages should be received.
   * Default: ReceiveMode.peekLock
   */
  receiveMode: ReceiveMode;
  /**
   * @property {number} maxConcurrentCalls he maximum number of messages that should be
   * processed concurrently while in peek lock mode. Once this limit has been reached, more
   * messages will not be received until messages currently being processed have been settled.
   * Default: 1
   */
  maxConcurrentCalls: number;

  /**
   * Instantiates a client pointing to the ServiceBus Subscription given by this configuration.
   *
   * @constructor
   * @param {string} topicPath The Topic path.
   * @param {string} subscriptionName The Subscription name.
   * @param {ConnectionContext} context The connection context to create the SubscriptionClient.
   * @param {SubscriptionClientOptions} [options] The SubscriptionClient options.
   */
  constructor(topicPath: string, subscriptionName: string, context: ConnectionContext, options?: SubscriptionClientOptions) {
    super(`${topicPath}/Subscriptions/${subscriptionName}`, context);
    if (!options) options = {};
    this.topicPath = topicPath;
    this.subscriptionName = subscriptionName;
    this.receiveMode = options.receiveMode || ReceiveMode.peekLock;
    this.maxConcurrentCalls = options.maxConcurrentCalls || 1;
  }

  /**
   * Closes the AMQP connection to the ServiceBus Subscription for this client,
   * returning a promise that will be resolved when disconnection is completed.
   * @returns {Promise<any>}
   */
  async close(): Promise<any> {
    try {
      if (this._context.namespace.connection && this._context.namespace.connection.isOpen()) {
        // Close the receiver.
        if (this._context.streamingReceiver) {
          await this._context.streamingReceiver.close();
        }
        // Close the management session
        await this._context.managementSession!.close();
        debug("Closed the subscription client '%s'.", this.id);
      }
    } catch (err) {
      const msg = `An error occurred while closing the subscription client ` +
        `"${this.id}": ${JSON.stringify(err)} `;
      debug(msg);
      throw new Error(msg);
    }
  }

  /**
   * Starts the receiver by establishing an AMQP session and an AMQP receiver link on the session.
   * Messages will be passed to the provided onMessage handler and error will be passed to the
   * provided onError handler.
   *
   * @param {OnMessage} onMessage          The message handler to receive Message objects.
   * @param {OnError} onError              The error handler to receive an error that occurs
   * while receiving messages.
   * @param {ReceiveOptions} [options]     Options for how you'd like to connect.
   *
   * @returns {ReceiveHandler} ReceiveHandler - An object that provides a mechanism to stop
   * receiving more messages.
   */
  receive(onMessage: OnMessage, onError: OnError, options?: MessageHandlerOptions): ReceiveHandler {
    if (!this._context.streamingReceiver ||
      (this._context.streamingReceiver && !this._context.streamingReceiver.isOpen())) {
      if (!options) options = {};
      const rcvOptions: ReceiveOptions = {
        maxConcurrentCalls: this.maxConcurrentCalls,
        receiveMode: this.receiveMode,
        autoComplete: options.autoComplete
      };
      const sReceiver = StreamingReceiver.create(this._context, rcvOptions);
      this._context.streamingReceiver = sReceiver;
      sReceiver.receive(onMessage, onError);
      return new ReceiveHandler(sReceiver);
    } else {
      const rcvr = this._context.streamingReceiver;
      const msg = `A "${rcvr.receiverType}" receiver with id "${rcvr.id}" has already been ` +
        `created for the Subscription "${this.name}". Another receive() call cannot be made while ` +
        `the previous one is active. Please stop the previous receive() by calling ` +
        `"receiveHandler.stop()".`;
      throw new Error(msg);
    }
  }

  /**
   * Receives a batch of Message objects from a ServiceBus Subscription for a given count and a
   * given max wait time in seconds, whichever happens first.
   * @param {number} maxMessageCount        The maximum message count. Must be a value greater than 0.
   * @param {number} [maxWaitTimeInSeconds] The maximum wait time in seconds for which the Receiver
   * should wait to receiver the said amount of messages. If not provided, it defaults to 60 seconds.
   * @param {ReceiveOptions} [options]      Options for how you'd like to connect.
   *
   * @returns {Promise<Message[]>} A promise that resolves with an array of Message objects.
   */
  async receiveBatch(maxMessageCount: number, maxWaitTimeInSeconds?: number): Promise<Message[]> {
    if (!this._context.batchingReceiver ||
      (this._context.batchingReceiver && !this._context.batchingReceiver.isOpen()) ||
      (this._context.batchingReceiver && !this._context.batchingReceiver.isReceivingMessages)) {
      const options: ReceiveOptions = {
        maxConcurrentCalls: this.maxConcurrentCalls,
        receiveMode: this.receiveMode
      };
      const bReceiver: BatchingReceiver = BatchingReceiver.create(this._context, options);
      this._context.batchingReceiver = bReceiver;
      let error: MessagingError | undefined;
      let result: Message[] = [];
      try {
        result = await bReceiver.receive(maxMessageCount, maxWaitTimeInSeconds);
      } catch (err) {
        error = err;
        debug("[%s] Receiver '%s', an error occurred while receiving %d messages for %d max time:\n %O",
          this._context.namespace.connectionId, bReceiver.id, maxMessageCount, maxWaitTimeInSeconds, err);
      }
      if (error) {
        throw error;
      }
      return result;
    } else {
      const rcvr = this._context.batchingReceiver;
      const msg = `A "${rcvr.receiverType}" receiver with id "${rcvr.id}" has already been ` +
        `created for the Subscription "${this.name}". Another receiveBatch() call cannot be made` +
        `while the previous one is active. Please wait for the previous receiveBatch() to complete` +
        `and then call receiveBatch() again.`;
      throw new Error(msg);
    }
  }
}
