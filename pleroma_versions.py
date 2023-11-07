import json
from datetime import datetime, time
from typing import Tuple, List, Sequence
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import fire
import requests
from requests import Response
from rich import print

from executor.long_task_executor import LongAsyncTaskExecutor


class PleromaVersions(object):

    def __init__(self, project_id: int, repository_id: int, max_workers: int = 5):
        self.executor = LongAsyncTaskExecutor(max_workers)
        self.repository_url = (f'https://git.pleroma.social/api/v4/projects/{project_id}'
                               f'/registry/repositories/{repository_id}/tags/'
                               )
        self.zone_info = ZoneInfo('Europe/Berlin')

    def list(self, after_date: str, before_date: str = None):
        if not self.repository_url:
            raise AttributeError('repository_url is required')

        filter_after_date = datetime.combine(datetime.strptime(after_date, '%Y-%m-%d'),
                                             time.min).astimezone(self.zone_info)
        filter_before_date = datetime.combine(before_date and (
                datetime.strptime(before_date, '%Y-%m-%d') or
                datetime.now()), time.max).astimezone(self.zone_info)

        print(
            f'Listing all tags between [{filter_after_date.strftime("%Y-%m-%d")}] '
            f'and [{filter_before_date.strftime('%Y-%m-%d')}]')

        tags = self._get_all_tags()
        filtered_tags = self._get_all_tag_details(tags, filter_after_date, filter_before_date)

        print(filtered_tags)

    def _get_all_tag_details(self, tags: Sequence, filter_after_date: datetime,
                             filter_before_date: datetime) -> Sequence:
        print(f'Getting details for [bold yellow]{len(tags)} tags')
        tags_details = []
        self.executor.execute(items=tags, item_func=self._tag_details, item_func_args=(),
                              callback=lambda result: tags_details.append(result))
        filtered_tags = list(
            filter(lambda tag: filter_after_date <= datetime.fromisoformat(tag['created_at']) <= filter_before_date,
                   tags_details))
        return filtered_tags

    def _get_all_tags(self) -> Sequence:
        print(f'Getting all pages from [[bold yellow]{self.repository_url}[/bold yellow]]')
        response, tags = self._get_tags()
        current_page = int(response.headers.get('X-Page'))
        total_pages = int(response.headers.get('X-Total-Pages'))
        self.executor.execute(items=range(current_page + 1, total_pages + 1),
                              item_func=self._get_tags,
                              item_func_args=(),
                              callback=lambda result: tags.extend(result[1]))
        return tags

    def _get_tags(self, page: int = 1) -> Tuple[Response, List]:
        page_url = self.repository_url + f'?page={page}'
        response = requests.get(page_url)
        response.raise_for_status()
        return response, json.loads(response.content)

    def _tag_details(self, tag):
        details_url = urljoin(self.repository_url, tag['name'])
        details_response = requests.get(details_url)
        details_response.raise_for_status()
        tag_details = json.loads(details_response.content)
        return tag_details


if __name__ == '__main__':
    fire.Fire(PleromaVersions)
