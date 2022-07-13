import { EventHubConsumerClient, latestEventPosition, ReceivedEventData } from "@azure/event-hubs";
import { createServer, Socket } from "net";
import { config } from "dotenv";

config();

const { CONNECTION_STRING, EVENT_HUB_NAME, TCP_PORT } = process.env;
if (CONNECTION_STRING === undefined) throw new Error("Enviroment variable CONNECTION_STRING is not defined");
if (EVENT_HUB_NAME === undefined) throw new Error("Enviroment variable EVENT_HUB_NAME is not defined");
if (TCP_PORT === undefined) throw new Error("Enviroment variable TCP_PORT is not defined");

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
