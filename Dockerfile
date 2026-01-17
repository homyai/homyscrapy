# Use the official Playwright image which includes Python and browsers
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Set working directory
WORKDIR /app

# Copy requirements first to leverage cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Install browsers (if not fully covered by the base image, but usually it is)
# playwright install is often needed if specific versions match
RUN playwright install chromium

# Set the entrypoint to a shell or scrapy
CMD ["scrapy", "list"]