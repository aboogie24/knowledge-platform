"""GitHub client for fetching Wiki.js docs."""

import base64
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx
import structlog

from ingestion.config import settings

logger = structlog.get_logger()


@dataclass
class GitHubFile:
    """Represents a file from GitHub."""

    path: str
    name: str
    sha: str
    content: str
    size: int
    url: str
    last_modified: datetime | None = None


class GitHubClient:
    """Client for fetching docs from GitHub."""

    def __init__(self):
        self.api_url = settings.github_api_url
        self.repo = settings.github_repo
        self.branch = settings.github_branch
        self.docs_path = settings.github_docs_path
        self.headers = {
            "Authorization": f"Bearer {settings.github_token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    async def get_tree(self, path: str = "") -> list[dict]:
        """Get repository tree (file listing)."""
        # Handle root directory case
        if self.docs_path in (".", "", "/"):
            target_path = path.strip("/") if path else ""
        else:
            target_path = f"{self.docs_path}/{path}".strip("/") if path else self.docs_path

        async with httpx.AsyncClient() as client:
            # Get the tree SHA for the branch
            url = f"{self.api_url}/repos/{self.repo}/git/trees/{self.branch}?recursive=1"
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            # Filter to only markdown files in docs path
            files = []
            for item in data.get("tree", []):
                if item["type"] != "blob":
                    continue
                    
                # Check file extension
                if not item["path"].endswith((".md", ".markdown", ".html")):
                    continue
                
                # Check path prefix (empty target_path means all files)
                if target_path and not item["path"].startswith(target_path):
                    continue
                    
                files.append(item)

            logger.info("fetched_tree", path=target_path or "(root)", file_count=len(files))
            return files

    async def get_file(self, path: str) -> GitHubFile:
        """Fetch a single file's content."""
        async with httpx.AsyncClient() as client:
            url = f"{self.api_url}/repos/{self.repo}/contents/{path}?ref={self.branch}"
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            # Decode base64 content
            content = base64.b64decode(data["content"]).decode("utf-8")

            # Get last commit date for this file
            last_modified = await self._get_last_modified(path)

            return GitHubFile(
                path=data["path"],
                name=Path(data["path"]).stem,
                sha=data["sha"],
                content=content,
                size=data["size"],
                url=data["html_url"],
                last_modified=last_modified,
            )

    async def _get_last_modified(self, path: str) -> datetime | None:
        """Get the last commit date for a file."""
        try:
            async with httpx.AsyncClient() as client:
                url = f"{self.api_url}/repos/{self.repo}/commits"
                params = {"path": path, "sha": self.branch, "per_page": 1}
                response = await client.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                commits = response.json()

                if commits:
                    date_str = commits[0]["commit"]["committer"]["date"]
                    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception as e:
            logger.warning("failed_to_get_last_modified", path=path, error=str(e))

        return None

    async def get_all_docs(self) -> list[GitHubFile]:
        """Fetch all markdown documents from the docs path."""
        tree = await self.get_tree()
        docs = []

        for item in tree:
            try:
                doc = await self.get_file(item["path"])
                docs.append(doc)
                logger.debug("fetched_doc", path=doc.path, size=doc.size)
            except Exception as e:
                logger.error("failed_to_fetch_doc", path=item["path"], error=str(e))

        logger.info("fetched_all_docs", count=len(docs))
        return docs

    async def get_changed_files(self, before_sha: str, after_sha: str) -> list[str]:
        """Get list of changed files between two commits."""
        async with httpx.AsyncClient() as client:
            url = f"{self.api_url}/repos/{self.repo}/compare/{before_sha}...{after_sha}"
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            changed = []
            for file in data.get("files", []):
                if file["filename"].startswith(self.docs_path):
                    if file["filename"].endswith((".md", ".markdown", ".html")):
                        changed.append(file["filename"])

            logger.info(
                "detected_changes",
                before=before_sha[:7],
                after=after_sha[:7],
                changed_count=len(changed),
            )
            return changed