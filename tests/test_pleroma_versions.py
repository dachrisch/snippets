import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

from more_itertools import one

from pleroma_versions import PleromaVersions


class TestPleromaVersions(unittest.TestCase):
    project_id = 123
    repository_id = 456
    max_workers = 5

    def setUp(self):
        self.pv = PleromaVersions(project_id=self.project_id, repository_id=self.repository_id,
                                  max_workers=self.max_workers)

    @patch('pleroma_versions.requests.get')
    def test_get_all_tags_successful(self, mock_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {'X-Page': '1', 'X-Total-Pages': '1'}
        mock_response.content = b'[{"name": "tag1", "created_at": "2020-01-01T00:00:00Z"}]'
        mock_get.return_value = mock_response

        tags = self.pv._get_all_tags()

        # Assert that requests.get was called with the expected URL
        mock_get.assert_called_with(
            f'https://git.pleroma.social/api/v4/projects/{self.project_id}'
            f'/registry/repositories/{self.repository_id}/tags/?page=1'
        )

        # Assert that the tags were correctly retrieved
        self.assertEqual(tags, [{"name": "tag1", "created_at": "2020-01-01T00:00:00Z"}])

    @patch('pleroma_versions.LongAsyncTaskExecutor.execute')
    def test_get_all_tag_details(self, mock_execute):
        # Prepare a mock for the tags that would be passed to the executor
        mock_tags = [{'name': 'tag1'},
                     {'name': 'tag2'}]

        # Mock tag details that would be returned by the item function
        mock_tag_details = [{'name': 'tag1', 'created_at': '2023-10-01T22:52:53.372+00:00'},
                            {'name': 'tag2', 'created_at': '2023-09-01T22:52:53.372+00:00'}]

        # Mock the callback inside the LongAsyncTaskExecutor.execute method
        def mock_execute_callback(items, item_func, item_func_args, callback):
            for tag in items:
                # Simulate the item function returning tag details
                callback(one(filter(lambda i: i['name'] == tag['name'], mock_tag_details)))

        # Set the side effect of the mock execute to our mock callback function
        mock_execute.side_effect = mock_execute_callback

        # Run the method under test
        filter_after_date = datetime(2023, 9, 5, tzinfo=ZoneInfo('Europe/Berlin'))
        filtered_tags = self.pv._get_all_tag_details(mock_tags, filter_after_date)

        # Assert that the execute method was called on the executor
        mock_execute.assert_called()

        # Assert that only tags after the filter date are included
        self.assertEqual(1, len(filtered_tags))
        self.assertEqual(filtered_tags[0]['name'], 'tag1')


if __name__ == '__main__':
    unittest.main()
