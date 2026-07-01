import logging
from playwright.async_api import async_playwright

logger = logging.getLogger("NexusKernel.Browser")

class AutonomousBrowserTools:
    _playwright = None
    _browser = None
    _context = None
    _page = None

    @classmethod
    async def initialize(cls):
        """Initializes a persistent, reusable single-user browser allocation block."""
        if cls._browser is None:
            logger.info("Spawning background Chromium engine runtime...")
            cls._playwright = await async_playwright().start()
            # Launching with a clean default profile configuration
            cls._browser = await cls._playwright.chromium.launch(headless=True)
            cls._context = await cls._browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            cls._page = await cls._context.new_page()
            logger.info("Browser engine initialized and locked to session framework.")

    @classmethod
    async def browse_to(cls, url: str) -> str:
        """Navigates to a target URL, waits for structural DOM settlement, and returns raw text content."""
        await cls.initialize()
        logger.info(f"Navigating browser core target network layer -> {url}")
        
        try:
            # Navigate with a defensive 30-second network loading window timeout
            await cls._page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Extract clean semantic text, stripping out useless design layouts and heavy scripts
            page_text = await cls._page.evaluate("() => document.body.innerText")
            page_title = await cls._page.title()
            
            # Return a highly compressed layout summary to keep Groq's context clean
            return f"Page Title: {page_title}\n\nRaw Text Ingested Content:\n{page_text[:4000]}"
        except Exception as e:
            logger.error(f"Browser navigation transaction failed: {str(e)}")
            return f"Network routing error: Unable to load target resource vector. Details: {str(e)}"

    @classmethod
    async def shutdown(cls):
        """Gracefully tears down open socket network bindings on system exit signals."""
        if cls._browser:
            await cls._browser.close()
            await cls._playwright.stop()
            logger.info("Browser systems closed down cleanly.")