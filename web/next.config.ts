import type { NextConfig } from "next";

// order-service serves the API. Proxying it under /api keeps the browser on a
// single origin, which is why there is no CORS handling anywhere in the Go
// code — and it is how a front end sits behind a gateway in production.
const config: NextConfig = {
  async rewrites() {
    const api = process.env.API_URL ?? "http://localhost:8090";
    return [{ source: "/api/:path*", destination: `${api}/:path*` }];
  },
};

export default config;
