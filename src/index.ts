import { EventHubConsumerClient, latestEventPosition, ReceivedEventData } from "@azure/event-hubs";
import { createServer, Socket } from "net";
import { envOrThrow } from "@inrixia/helpers/object";

import { config } from "dotenv";
config();

const TCP_PORT = envOrThrow("TCP_PORT");
const CONNECTION_STRING = envOrThrow("CONNECTION_STRING");
const EVENT_HUB_NAME = envOrThrow("EVENT_HUB_NAME");

const sockets: Record<string, Socket> = {};
createServer()
	.listen(+TCP_PORT, () => console.log(`Listening on port ${TCP_PORT}`))
	.on("connection", (socket) => {
		if (socket.remoteAddress === undefined || socket.remotePort === undefined) return socket.destroy();

		const idx = `${socket.remoteAddress}:${socket.remotePort}`;
		sockets[idx] = socket;
		console.log(`Client ${idx} connected.`);

		const cleanup = () => {
			socket.destroy();
			delete sockets[idx];
		};

		socket
			.on("end", () => {
				console.log(`Client ${socket.remoteAddress} disconnected.`);
				cleanup();
			})
			.on("error", (err) => {
				console.error(`Client ${socket.remoteAddress} encountered a error: ${err}`);
				cleanup();
			});
	});

const reduceEvents = (events: ReceivedEventData[]) => {
	let str = "";
	for (const event of events) str += `${event.body}\r\n`;
	return str;
};

const main = async () => {
	const client = new EventHubConsumerClient("$Default", CONNECTION_STRING, EVENT_HUB_NAME);
	const sub = client.subscribe(
		{
			processEvents: async (events) => {
				const str = reduceEvents(events);
				for (const idx in sockets) sockets[idx].write(str);
			},
			processError: async (err, context) => console.error(err, context),
		},
		{ startPosition: latestEventPosition }
	);

	const lifeCheck = async () => {
		if (!sub.isRunning) {
			console.log("Subscription is not running, attempting to restart...");
			// Close sub and client
			await sub.close();
			await client.close();
			// Restart
			main();
			return;
		}
		setTimeout(lifeCheck, 1000);
	};
	lifeCheck();
};
main();
