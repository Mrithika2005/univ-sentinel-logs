import express from "express";
import path from "path";
import { createServer as createViteServer } from "vite";
import { initSentinel } from "@/sentinel-sdk/node/agent.ts";

/**
 * STEP 1: INITIALIZE SENTINEL SDK
 * This hooks into http/express/fs automatically
 */
const sentinel = initSentinel("Main-API-Backend");

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(express.json());

  // API Layers: Demonstrating Business Logic Layer
  app.get("/api/data", (req, res) => {
    // This console.log will be captured by Sentinel as BUSINESS_LOGIC layer
    console.log("Fetching sample data from database simulation...");
    
    setTimeout(() => {
      res.json({
        success: true,
        data: ["Item 1", "Item 2", "Item 3"],
        source: "Sentinel Monitoring System"
      });
    }, 100);
  });

  // Layered Architecture Simulation: Security Layer Check
  app.post("/api/admin", (req, res) => {
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      console.warn("Unauthorized access attempt to administration endpoint");
      return res.status(401).json({ error: "Unauthorized" });
    }
    res.json({ message: "Admin access granted" });
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
