# Local setup

1. Add your OpenAI key to an environment variable:

```bash
export OPENAI_API_KEY="YOUR_KEY_HERE"
```

2. Install local dependencies:

```bash
npm install
npm run install:browsers
```

3. (Optional) choose a model, port, and rendered-fetch mode:

```bash
export OPENAI_MODEL="gpt-4.1-mini"
export PORT=8787
export RENDERED_FETCH="always"
```

Use `RENDERED_FETCH=auto` to render only when normal fetch looks weak, or `RENDERED_FETCH=off` to disable browser rendering.

4. Start the local server:

```bash
npm run dev
```

5. Open the app:

```text
http://localhost:8787/dashboard.html
```

or

```text
http://localhost:8787/scraper/jobs.html
```

The URL scanner calls `POST /api/extract-jobs`, which fetches the page and asks OpenAI to extract:
- company
- job title
- posted date
- link

The server also tries structured ATS APIs for Workday, Greenhouse, Lever, Ashby, SmartRecruiters, Teamtailor, Recruitee, and Breezy before falling back to normal HTML/Jina fetch and Playwright-rendered pages. It does not bypass CAPTCHA, login walls, or explicit anti-bot blocking.
