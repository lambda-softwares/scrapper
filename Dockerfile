FROM --platform=linux/amd64 python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies + Google Chrome
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg2 \
        # Chrome dependencies
        libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
        libcairo2 libcups2 libdbus-1-3 libexpat1 libgbm1 \
        libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 \
        libudev1 libvulkan1 libx11-6 libxcb1 libxcomposite1 \
        libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 \
        libcurl4 fonts-liberation xdg-utils && \
    wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i /tmp/google-chrome.deb || apt-get install -f -y && \
    rm /tmp/google-chrome.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy dependency files and install
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY *.py ./

# Create logs directory
RUN mkdir -p logs

ENTRYPOINT ["uv", "run", "python", "parallel_coordinator.py", "--workers", "2", "--resume"]
