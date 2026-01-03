# Use Python 3.10 slim image
FROM python:3.10-slim-bookworm

# 1. Install System Dependencies & FFmpeg
# We install git to fetch pyrofork if needed, and ffmpeg for video processing
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. Set Working Directory
WORKDIR /app

# 3. Copy Requirements first (Better caching)
COPY requirements.txt .

# 4. Install Python Libraries
RUN pip3 install --no-cache-dir -r requirements.txt

# 5. Copy the Bot Code
COPY . .

# 6. Command to run the bot
CMD ["python3", "main.py"]