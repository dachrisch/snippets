import concurrent
import concurrent.futures
import json
from datetime import datetime
from typing import Tuple, List
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import fire
import requests
from alive_progress import alive_bar
from requests import Response
from rich import print


class PleromaVersions(object):

    def __init__(self, project_id: int, repository_id: int, max_workers: int = 5):
        self.max_workers = max_workers
        self.repository_url = (f'https://git.pleroma.social/api/v4/projects/{project_id}'
                               f'/registry/repositories/{repository_id}/tags/'
                               )
        print(f'Getting all pages from [[bold yellow]{self.repository_url}[/bold yellow]]')

    def list(self, after_date: str):
        if not self.repository_url:
            raise AttributeError('repository_url is required')

        filter_after_date = datetime.strptime(after_date, '%Y-%m-%d').astimezone(ZoneInfo('Europe/Berlin'))

        response, tags = self._get_tags(self.repository_url)
        current_page = int(response.headers.get('X-Page'))
        total_pages = int(response.headers.get('X-Total-Pages'))

        with alive_bar(total_pages) as bar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                tag_futures = [executor.submit(self._get_tags, self.repository_url, page) for page in
                               range(current_page, total_pages + 1)]

                for tag_future in concurrent.futures.as_completed(tag_futures):
                    tags.extend(tag_future.result()[1])
                    bar()

        print(f'Getting details for [bold yellow]{len(tags)} tags')

        tags_details = []
        with alive_bar(len(tags)) as bar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                tag_futures = [executor.submit(self._tag_details, tag) for tag in tags]
                for tag_future in concurrent.futures.as_completed(tag_futures):
                    tags_details.append(tag_future.result())
                    bar()
        filtered_tags = list(
            filter(lambda tag: filter_after_date < datetime.fromisoformat(tag['created_at']), tags_details))
        print(filtered_tags)

    def _get_tags(self, base_url: str, page: int = 1) -> Tuple[Response, List]:
        page_url = base_url + f'?page={page}'
        response = requests.get(page_url)
        if response.status_code != 200:
            raise ValueError(f'Invalid response while calling [{page_url}]: {response}')
        return response, json.loads(response.content)

    def _tag_details(self, tag):
        details_url = urljoin(self.repository_url, tag['name'])
        details_response = requests.get(details_url)
        if details_response.status_code != 200:
            raise ValueError(f'Couldn\'t fetch details [{details_url}] for tag {tag}: {details_response}')
        tag_details = json.loads(details_response.content)
        return tag_details


if __name__ == '__main__':
    fire.Fire(PleromaVersions)
