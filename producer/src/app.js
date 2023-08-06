import express from "express";
import { Kafka } from "kafkajs";
import dotenv from "dotenv";

dotenv.config;

const PORT = 3000;
const app = express();

// setup kafka
const kafka = new Kafka({
  clientId: "tutorial-producer",
  brokers: [process.env.BROKER], // to be replaced with your Kafka broker(s) address e.g localhost:9092 if you have kafka installed locally
});

// create producer
const producer = kafka.producer();

producer.on("producer.connect", () => {
  console.log("Producer connected is ready");
});

// send message to consumer
const sendMessage = async (message, topic) => {
  try {
    await producer.connect();
    await producer.send({
      topic: topic,
      messages: [message],
    });
  } catch (error) {
    console.error(error);
  } finally {
    await producer.disconnect();
  }
};

app.get("/", (req, res) => {
  res.send({ message: "express kafka tutorial" });
});

app.post("/send", async (req, res) => {
  const { name, email, mobileNumber } = req.body;

  const message = { name: name, email: email, mobileNumber: mobileNumber };

  await sendMessage(message, "tutorial-topic");
  res.send({ message: "message sent" });
});

app.listen(PORT, () => {
  console.log(`server running on port ${PORT}`);
});
