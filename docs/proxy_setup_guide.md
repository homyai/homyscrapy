# Residential Proxies Guide

## What are Residential Proxies?

Unlike **Datacenter Proxies** (which come from cloud servers like AWS or DigitalOcean and are easily detected), **Residential Proxies** route your traffic through real devices (home Wi-Fi, mobile data) assigned by Internet Service Providers (ISPs) to real people.

### Why do you need them?
Websites like **Encuentra24**, **Century21**, and **InHaus** use security systems (Cloudflare, Imperva) to detect bots.
- **Datacenter IP**: "High Risk" (Bots usually come from servers).
- **Residential IP**: "Low Risk" (Looks like a normal user browsing from home).

Using a residential proxy makes your scraper indistinguishable from a regular visitor.

## How to Get Them

You need to purchase access from a **Proxy Agent/Provider**. Here are some reputable ones:

1.  **Smartproxy** (Good balance of price/performance)
2.  **Bright Data** (Premium, very robust)
3.  **IPRoyal** (Often cheaper, pay-as-you-go options)
4.  **Oxylabs** (Enterprise grade)

### Steps to Acquire:

1.  **Sign Up**: Create an account with one of the providers above.
2.  **Buy a Plan**: Look for "Residential Proxies". Many offer "Pay as you go" (pay per GB of data) which is perfect for development.
3.  **Create a User**: In their dashboard, go to "Proxy Setup" or "User Management".
    *   Select "Residential".
    *   Create a "Proxy User" (username and password).
4.  **Get Credentials**: They will provide you with:
    *   **Host**: e.g., `pr.oxylabs.io` or `gate.smartproxy.com`
    *   **Port**: e.g., `7777`
    *   **Username**: e.g., `user-scop-1234`
    *   **Password**: `your_secret_password`

## Configuration

Once you have these details, construct the `PROXY_SERVER` URL format:
`http://{host}:{port}`

And update your `.devcontainer/.env` file:

```ini
# .devcontainer/.env

# The address of the provider's gateway
PROXY_SERVER=http://gate.smartproxy.com:7000

# The user you created in their dashboard
PROXY_USER=myusername

# The password for that user
PROXY_PASSWORD=mypassword
```

Your `encuentra24.py` spider is already configured to read these values and authenticate automatically.
