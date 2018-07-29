import { OnMessage, OnError, MessagingError, delay, Message, ReceiveMode, Namespace } from "../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env["SERVICEBUS_CONNECTION_STRING"] || "";
const path = process.env["QUEUE_NAME"] || "";

console.log("str: ", str);
console.log("path: ", path);

async function main(): Promise<void> {
  const ns: Namespace = Namespace.createFromConnectionString(str);
  const client = ns.createQueueClient(path, { receiveMode: ReceiveMode.peekLock, maxConcurrentCalls: 1 });

  console.log("Created listener");

  const onMessage: OnMessage = async (brokeredMessage: Message) => {
    console.log(">>>> Message: ", brokeredMessage);

    brokeredMessage.complete();
  }

  const onError: OnError = (err: MessagingError | Error) => {
    console.log(">>>> Error occurred: ", err);
  };

  client.receive(onMessage, onError, { autoComplete: true });

  console.log("Listening for messages");

  // give the receiver some time to consume messages
  await delay(5000);

  return ns.close();
}

main().catch((err) => console.log("error: ", err));
