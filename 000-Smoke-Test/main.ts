import { copy } from "https://deno.land/std@0.151.0/streams/conversion.ts";

const port = 7;
const listener = Deno.listen({ port });
console.log(`Listening on port ${port}.`);
for await (const conn of listener) {
    copy(conn, conn).finally(() => conn.close());
}