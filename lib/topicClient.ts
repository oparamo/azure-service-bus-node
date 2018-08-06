// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

import * as debugModule from "debug";
import { ConnectionContext } from "./connectionContext";
import { MessageSender } from "./messageSender";
import { ServiceBusMessage } from "./message";
import { Client } from "./client";

const debug = debugModule("azure:service-bus:topic-client");

export class TopicClient extends Client {
  /**
   * Instantiates a client pointing to the ServiceBus Topic given by this configuration.
   *
   * @constructor
   * @param {string} name The topic name.
   * @param {ConnectionContext} context The connection context to create the TopicClient.
   * @param {TopicClientOptions} [options] The TopicClient options.
   */
  constructor(name: string, context: ConnectionContext) {
    super(name, context);
  }

  /**
   * Closes the AMQP connection to the ServiceBus Topic for this client,
   * returning a promise that will be resolved when disconnection is completed.
   * @returns {Promise<any>}
   */
  async close(): Promise<any> {
    try {
      if (this._context.namespace.connection && this._context.namespace.connection.isOpen()) {
        // Close the sender.
        if (this._context.sender) {
          await this._context.sender.close();
        }
        debug("Closed the topic client '%s'.", this.id);
      }
    } catch (err) {
      const msg = `An error occurred while closing the topic client ` +
        `"${this.id}": ${JSON.stringify(err)} `;
      debug(msg);
      throw new Error(msg);
    }
  }

  /**
   * Sends the given message to the ServiceBus Topic.
   *
   * @param {any} data  Message to send.  Will be sent as UTF8-encoded JSON string.
   * @returns {Promise<void>} Promise<void>
   */
  async send(data: ServiceBusMessage): Promise<void> {
    const sender = MessageSender.create(this._context);
    await sender.send(data);

    return Promise.resolve();
  }

  /**
   * Send a batch of Message to the ServiceBus Topic. The "message_annotations",
   * "application_properties" and "properties" of the first message will be set as that of
   * the envelope (batch message).
   *
   * @param {Array<Message>} datas  An array of Message objects to be sent in a Batch
   * message.
   *
   * @return {Promise<void>} Promise<void>
   */
  async sendBatch(datas: ServiceBusMessage[]): Promise<void> {
    const sender = MessageSender.create(this._context);
    await sender.sendBatch(datas);

    return Promise.resolve();
  }
}
