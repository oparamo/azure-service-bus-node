import { Namespace } from "../lib";
import * as dotenv from "dotenv";
dotenv.config();

const str = process.env["SERVICEBUS_CONNECTION_STRING"] || "";
const path = process.env["QUEUE_NAME"] || "";

console.log("str: ", str);
console.log("path: ", path);

async function main(): Promise<void> {
  const ns: Namespace = Namespace.createFromConnectionString(str);
  const client = ns.createQueueClient(path);

  console.log(">>>> Created sender");

  await client.send({ body: "Hello sb world!!" });

  console.log(">>>> Sent the message");
  console.log(">>>> Closing connections");

  return ns.close();
}

main().catch((err) => console.log("error: ", err));
