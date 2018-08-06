// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

import * as debugModule from "debug";
import * as uuid from "uuid/v4";
import { LinkEntity } from "./linkEntity";
import { ClientEntityContext } from "./clientEntityContext";
import {
  messageProperties, Sender, EventContext, OnAmqpEvent, SenderOptions, Delivery, SenderEvents,
  message
} from "./rhea-promise";
import { defaultLock, Func, retry, translate, AmqpMessage } from "./amqp-common";
import { ServiceBusMessage } from "./message";

const debug = debugModule("azure:service-bus:sender");

/**
 * Describes the MessageSender that will send messages to ServiceBus.
 * @class MessageSender
 */
export class MessageSender extends LinkEntity {
  /**
   * @property {string} senderLock The unqiue lock name per connection that is used to acquire the
   * lock for establishing a sender link by an entity on that connection.
   * @readonly
   */
  readonly senderLock: string = `sender-${uuid()}`;
  /**
   * @property {any} [_sender] The AMQP sender link.
   * @private
   */
  private _sender?: Sender;

  /**
   * Creates a new MessageSender instance.
   * @constructor
   * @param {ClientEntityContext} context The client entity context.
   */
  constructor(context: ClientEntityContext) {
    super(`${context.entityPath}`, context);
    this.address = this._context.entityPath as string;
    this.audience = `${this._context.namespace.config.endpoint}${this.address}`;
  }

  /**
   * "Unlink" this sender, closing the link and resolving when that operation is complete.
   * Leaves the underlying connection open.
   * @return {Promise<void>} Promise<void>
   */
  async close(): Promise<void> {
    if (this._sender) {
      try {
        await this._sender.close();
        this._context.sender = undefined;
        debug("[%s] Deleted the sender '%s' with address '%s' from the client entity context.",
          this._context.namespace.connectionId, this.id, this.address);
        this._sender = undefined;
        clearTimeout(this._tokenRenewalTimer as NodeJS.Timer);
        debug("[%s]Sender '%s' closed.", this._context.namespace.connectionId, this.id);
      } catch (err) {
        debug("An error occurred while closing the sender %O", err);
        throw err;
      }
    }
  }

  /**
   * Sends the given message, with the given options on this link
   *
   * @param {any} data Message to send.  Will be sent as UTF8-encoded JSON string.
   * @returns {Promise<Delivery>} Promise<Delivery>
   */
  async send(data: ServiceBusMessage): Promise<Delivery> {
    try {
      if (!data || (data && typeof data !== "object")) {
        throw new Error("data is required and it must be of type object.");
      }

      if (!this._isOpen()) {
        debug("Acquiring lock %s for initializing the session, sender and " +
          "possibly the connection.", this.senderLock);
        await defaultLock.acquire(this.senderLock, () => { return this._init(); });
      }
      const message = ServiceBusMessage.toAmqpMessage(data);
      message.body = this._context.namespace.dataTransformer.encode(data.body);
      return await this._trySend(message);
    } catch (err) {
      debug("An error occurred while sending the message %O", err);
      throw err;
    }
  }

  /**
   * Send a batch of Message to the ServiceBus. The "message_annotations",
   * "application_properties" and "properties" of the first message will be set as that
   * of the envelope (batch message).
   * @param {Array<Message>} datas  An array of Message objects to be sent in a
   * Batch message.
   * @return {Promise<Delivery>} Promise<Delivery>
   */
  async sendBatch(datas: ServiceBusMessage[]): Promise<Delivery> {
    try {
      if (!datas || (datas && !Array.isArray(datas))) {
        throw new Error("data is required and it must be an Array.");
      }

      if (!this._isOpen()) {
        debug("Acquiring lock %s for initializing the session, sender and " +
          "possibly the connection.", this.senderLock);
        await defaultLock.acquire(this.senderLock, () => { return this._init(); });
      }
      debug("[%s] Sender '%s', trying to send Message[]: %O",
        this._context.namespace.connectionId, this.id, datas);
      const messages: AmqpMessage[] = [];
      // Convert Message to AmqpMessage.
      for (let i = 0; i < datas.length; i++) {
        const message = ServiceBusMessage.toAmqpMessage(datas[i]);
        message.body = this._context.namespace.dataTransformer.encode(datas[i].body);
        messages[i] = message;
      }
      // Encode every amqp message and then convert every encoded message to amqp data section
      const batchMessage: AmqpMessage = {
        body: message.data_sections(messages.map(message.encode))
      };
      // Set message_annotations, application_properties and properties of the first message as
      // that of the envelope (batch message).
      if (messages[0].message_annotations) {
        batchMessage.message_annotations = messages[0].message_annotations;
      }
      if (messages[0].application_properties) {
        batchMessage.application_properties = messages[0].application_properties;
      }
      for (const prop of messageProperties) {
        if ((messages[0] as any)[prop]) {
          (batchMessage as any)[prop] = (messages[0] as any)[prop];
        }
      }

      // Finally encode the envelope (batch message).
      const encodedBatchMessage = message.encode(batchMessage);
      debug("[%s]Sender '%s', sending encoded batch message.",
        this._context.namespace.connectionId, this.id, encodedBatchMessage);
      return await this._trySend(encodedBatchMessage, undefined, 0x80013700);
    } catch (err) {
      debug("An error occurred while sending the batch message %O", err);
      throw err;
    }
  }

  /**
   * Tries to send the message to ServiceBus if there is enough credit to send them
   * and the circular buffer has available space to settle the message after sending them.
   *
   * We have implemented a synchronous send over here in the sense that we shall be waiting
   * for the message to be accepted or rejected and accordingly resolve or reject the promise.
   *
   * @param message The message to be sent to ServiceBus.
   * @return {Promise<Delivery>} Promise<Delivery>
   */
  private _trySend(message: ServiceBusMessage, tag?: any, format?: number): Promise<Delivery> {
    const sendEventPromise = new Promise<Delivery>((resolve, reject) => {
      debug("[%s] Sender '%s', credit: %d available: %d", this._context.namespace.connectionId,
        this.id, this._sender!.credit, this._sender!.session.outgoing.available());
      if (this._sender!.sendable()) {
        debug("[%s] Sender '%s', sending message: %O", this._context.namespace.connectionId, this.id, message);
        let onRejected: Func<EventContext, void>;
        let onReleased: Func<EventContext, void>;
        let onModified: Func<EventContext, void>;
        let onAccepted: Func<EventContext, void>;
        const removeListeners = (): void => {
          this._sender!.removeHandler(SenderEvents.rejected, onRejected);
          this._sender!.removeHandler(SenderEvents.accepted, onAccepted);
          this._sender!.removeHandler(SenderEvents.released, onReleased);
          this._sender!.removeHandler(SenderEvents.modified, onModified);
        };

        onAccepted = (context: EventContext) => {
          // Since we will be adding listener for accepted and rejected event every time
          // we send a message, we need to remove listener for both the events.
          // This will ensure duplicate listeners are not added for the same event.
          removeListeners();
          debug("[%s] Sender '%s', got event accepted.",
            this._context.namespace.connectionId, this.id);
          resolve(context.delivery);
        };
        onRejected = (context: EventContext) => {
          removeListeners();
          debug("[%s] Sender '%s', got event rejected.",
            this._context.namespace.connectionId, this.id);
          reject(translate(context!.delivery!.remote_state!.error));
        };
        onReleased = (context: EventContext) => {
          removeListeners();
          debug("[%s] Sender '%s', got event released.",
            this._context.namespace.connectionId, this.id);
          let err: Error;
          if (context!.delivery!.remote_state!.error) {
            err = translate(context!.delivery!.remote_state!.error);
          } else {
            err = new Error(`[${this._context.namespace.connectionId}]Sender '${this.id}', ` +
              `received a release disposition.Hence we are rejecting the promise.`);
          }
          reject(err);
        };
        onModified = (context: EventContext) => {
          removeListeners();
          debug("[%s] Sender '%s', got event modified.",
            this._context.namespace.connectionId, this.id);
          let err: Error;
          if (context!.delivery!.remote_state!.error) {
            err = translate(context!.delivery!.remote_state!.error);
          } else {
            err = new Error(`[${this._context.namespace.connectionId}]Sender "${this.id}", ` +
              `received a modified disposition.Hence we are rejecting the promise.`);
          }
          reject(err);
        };
        this._sender!.registerHandler(SenderEvents.accepted, onAccepted);
        this._sender!.registerHandler(SenderEvents.rejected, onRejected);
        this._sender!.registerHandler(SenderEvents.modified, onModified);
        this._sender!.registerHandler(SenderEvents.released, onReleased);
        const delivery = this._sender!.send(message, tag, format);
        debug("[%s] Sender '%s', sent message with delivery id: %d",
          this._context.namespace.connectionId, this.id, delivery.id);
      } else {
        const msg = `[${this._context.namespace.connectionId}]Sender "${this.id}", ` +
          `cannot send the message right now. Please try later.`;
        debug(msg);
        reject(new Error(msg));
      }
    });

    return retry<Delivery>(() => sendEventPromise);
  }

  /**
   * Determines whether the AMQP sender link is open. If open then returns true else returns false.
   * @private
   *
   * @return {boolean} boolean
   */
  private _isOpen(): boolean {
    return this._sender! && this._sender!.isOpen();
  }

  /**
   * Initializes the sender session on the connection.
   * @returns {Promise<void>}
   */
  private async _init(): Promise<void> {
    try {
      if (!this._isOpen()) {
        await this._negotiateClaim();
        const onAmqpError: OnAmqpEvent = (context: EventContext) => {
          const senderError = translate(context.sender!.error!);
          // TODO: Should we retry before calling user's error method?
          debug("[%s] An error occurred for sender '%s': %O.",
            this._context.namespace.connectionId, this.id, senderError);
        };
        debug("[%s] Trying to create sender '%s'...",
          this._context.namespace.connectionId, this.id);
        const options = this._createSenderOptions(onAmqpError);
        this._sender = await this._context.namespace.connection!.createSender(options);
        debug("[%s] Promise to create the sender resolved. Created sender with name: %s",
          this._context.namespace.connectionId, this.id);
        debug("[%s] Sender '%s' created with sender options: %O",
          this._context.namespace.connectionId, this.id, options);
        // It is possible for someone to close the sender and then start it again.
        // Thus make sure that the sender is present in the client cache.
        if (!this._context.sender) this._context.sender = this;
        await this._ensureTokenRenewal();
      }
    } catch (err) {
      err = translate(err);
      debug("[%s] An error occurred while creating the sender %s",
        this._context.namespace.connectionId, this.id, err);
      throw err;
    }
  }

  private _createSenderOptions(onError?: OnAmqpEvent): SenderOptions {
    const options: SenderOptions = {
      name: this.id,
      target: {
        address: this.address
      },
      onError: onError
    };
    debug("Creating sender with options: %O", options);
    return options;
  }

  /**
   * Creates a new sender to the given event hub, and optionally to a given partition if it is
   * not present in the context or returns the one present in the context.
   * @static
   * @returns {Promise<MessageSender>}
   */
  static create(context: ClientEntityContext): MessageSender {
    if (!context.sender) {
      context.sender = new MessageSender(context);
    }
    return context.sender;
  }
}
