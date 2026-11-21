// The Effect client connects at the first statement, not when its layer is built.
import { expect, test } from 'bun:test';
import { Context, Effect, Exit, Layer, ManagedRuntime, Redacted } from 'effect';
import net from 'node:net';
import { DrizzlePostgresClient } from '../../src/effect/index.ts';
import { createPool } from '../../src/index.ts';

/** A TCP server that counts the sockets it accepts and never answers them. */
const listen = async () => {
	let accepted = 0;
	const sockets = new Set<net.Socket>();
	const server = net.createServer((socket) => {
		accepted++;
		sockets.add(socket);
		socket.on('error', () => {});
	});
	await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
	return {
		port: (server.address() as net.AddressInfo).port,
		accepted: () => accepted,
		close: () => {
			for (const socket of sockets) socket.destroy();
			return new Promise<void>((resolve) => server.close(() => resolve()));
		},
	};
};

/** A port on 127.0.0.1 that refuses connections. */
const closedPort = async () => {
	const server = await listen();
	await server.close();
	return server.port;
};

const urlOf = (port: number) => Redacted.make(`postgres://user:pass@127.0.0.1:${port}/db`);

test('building the layer opens no connection', async () => {
	const server = await listen();
	const runtime = ManagedRuntime.make(DrizzlePostgresClient.layer({ url: urlOf(server.port), maxConnections: 1 }));
	try {
		const context = await runtime.context();
		// one turn of the event loop, so that a connection the layer had opened would have been accepted
		await new Promise((resolve) => setTimeout(resolve, 50));
		expect(server.accepted()).toBe(0);
		expect(Context.get(context, DrizzlePostgresClient.DrizzlePostgresClient).pool.size).toBe(0);
	} finally {
		await runtime.dispose();
		await server.close();
	}
});

test('the layer builds when the server is unreachable', async () => {
	const layer = DrizzlePostgresClient.layer({ url: urlOf(await closedPort()), maxConnections: 1 });
	const exit = await Effect.runPromise(Effect.exit(Effect.scoped(Layer.build(layer))));
	expect(Exit.isSuccess(exit)).toBe(true);
});

test('layerFromPool opens no connection on the pool', async () => {
	const server = await listen();
	const pool = createPool({ host: '127.0.0.1', port: server.port, user: 'user', database: 'db' });
	try {
		await Effect.runPromise(Effect.scoped(Layer.build(DrizzlePostgresClient.layerFromPool(pool))));
		expect(pool.size).toBe(0);
		expect(server.accepted()).toBe(0);
	} finally {
		await pool.end();
		await server.close();
	}
});

test('reconnect: false fails the first statement at once when the server is unreachable', async () => {
	const layer = DrizzlePostgresClient.layer({ url: urlOf(await closedPort()), maxConnections: 1, reconnect: false });
	const started = Date.now();
	const error = await Effect.runPromise(
		Effect.gen(function*() {
			const sql = yield* DrizzlePostgresClient.DrizzlePostgresClient;
			return yield* Effect.flip(sql`select 1`);
		}).pipe(Effect.provide(layer)),
	);
	expect(error.reason._tag).toBe('ConnectionError');
	expect(Date.now() - started).toBeLessThan(2000);
});

test('reconnect: { acquireTimeoutMs } bounds the wait for an unreachable server', async () => {
	const layer = DrizzlePostgresClient.layer({
		url: urlOf(await closedPort()),
		maxConnections: 1,
		reconnect: { acquireTimeoutMs: 200 },
	});
	const started = Date.now();
	const error = await Effect.runPromise(
		Effect.gen(function*() {
			const sql = yield* DrizzlePostgresClient.DrizzlePostgresClient;
			return yield* Effect.flip(sql`select 1`);
		}).pipe(Effect.provide(layer)),
	);
	expect(error.reason._tag).toBe('ConnectionError');
	expect(error.message).toMatch(/timed out after 200ms waiting for the database to recover/);
	expect(Date.now() - started).toBeLessThan(2000);
});

test('a config that the driver rejects fails the first statement with a ConnectionError', async () => {
	const layer = DrizzlePostgresClient.layer({ host: 'user:pass@127.0.0.1', port: await closedPort() });
	const tags = await Effect.runPromise(
		Effect.gen(function*() {
			const sql = yield* DrizzlePostgresClient.DrizzlePostgresClient;
			const statement = yield* Effect.flip(sql`select 1`);
			const bulk = yield* Effect.flip(sql.bulkInsert('t', { id: 'int4' }, [[1]]));
			return [statement.reason._tag, bulk._tag];
		}).pipe(Effect.provide(layer)),
	);
	expect(tags).toEqual(['ConnectionError', 'SqlError']);
});

test('a pool that does not record its server gives a client without the server span attributes', async () => {
	const pool = createPool({ host: '127.0.0.1', port: await closedPort(), user: 'user', database: 'db' });
	delete (pool as unknown as Record<symbol, unknown>)[Symbol.for('drizzle-postgres.poolServer')];
	try {
		const exit = await Effect.runPromise(
			Effect.exit(Effect.scoped(Layer.build(DrizzlePostgresClient.layerFromPool(pool)))),
		);
		expect(Exit.isSuccess(exit)).toBe(true);
	} finally {
		await pool.end();
	}
});
