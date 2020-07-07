import os
from typing import Dict, Iterable, Optional, Union, Any

import pandas as pd

from .api_helper import get_video_data_from_trending


def parse_video_data(video_data: Dict[str, Any]) -> Dict[str, Union[str, int, float]]:
    """
    Parse a responce from Youtube API for one video.
    
    Return a dictionary with the video info and statistics.
    """
    video_title = video_data["snippet"]["title"]
    channel_name = video_data["snippet"]["channelTitle"]

    channel_link_template = "https://www.youtube.com/channel/{}"
    channel_id = video_data["snippet"]["channelId"]
    channel_link = channel_link_template.format(channel_id)

    video_link_template = "https://www.youtube.com/watch?v={}"
    video_id = video_data["id"]
    video_link = video_link_template.format(video_id)

    published_ts = video_data["snippet"]["publishedAt"]
    published_ts = pd.Timestamp(published_ts)
    time_since_publishing = int(
        (pd.Timestamp.now(tz=published_ts.tz) - pd.Timestamp(published_ts))
        .round("H")
        .total_seconds()
        / 3600
    )

    try:
        video_thumbnail_link = video_data["snippet"]["thumbnails"]["standard"]["url"]
    except KeyError:
        possible_thumbnails_res = ["high", "maxres", "medium", "default"]
        video_thumbnail_link = None
        for thumbnail_res in possible_thumbnails_res:
            if thumbnail_res in video_data["snippet"]["thumbnails"]:
                if "url" in video_data["snippet"]["thumbnails"][thumbnail_res]:
                    video_thumbnail_link = video_data["snippet"]["thumbnails"][
                        thumbnail_res
                    ]["url"]
                    break
    tags = None
    if "tags" in video_data["snippet"]:
        tags = ",".join(video_data["snippet"]["tags"])

    blocked_in_countries = None
    if "regionRestriction" in video_data["contentDetails"]:
        if "blocked" in video_data["contentDetails"]["regionRestriction"]:
            blocked_in_countries = ",".join(
                video_data["contentDetails"]["regionRestriction"]["blocked"]
            )

    likes_count = -1
    if "likeCount" in video_data["statistics"]:
        likes_count = int(video_data["statistics"]["likeCount"])
    dislikes_count = -1
    if "dislikeCount" in video_data["statistics"]:
        dislikes_count = int(video_data["statistics"]["dislikeCount"])
    views_count = -1
    if "viewCount" in video_data["statistics"]:
        views_count = int(video_data["statistics"]["viewCount"])
    comments_count = -1
    if "commentCount" in video_data["statistics"]:
        comments_count = int(video_data["statistics"]["commentCount"])

    return {
        "video_title": video_title,
        "video_link": video_link,
        "video_id": video_id,
        "channel_link": channel_link,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "published_ts": published_ts,
        "hours_since_publishing": time_since_publishing,
        "tags": tags,
        "likes_count": likes_count,
        "dislikes_count": dislikes_count,
        "views_count": views_count,
        "comments_count": comments_count,
        "blocked_in_countries": blocked_in_countries,
        "video_thumbnail_link": video_thumbnail_link,
    }


def compose_dataframe(region: str, top_n_videos: int = 50) -> pd.DataFrame:
    """
    Get video data for `top_n_videos` in a `region` and 
    compose a dataframe from each video from Youtube trending page

    Return a pandas Dataframe.
    """
    data = get_video_data_from_trending(region=region, top_n_videos=top_n_videos)
    videos_df = (
        pd.DataFrame(list(map(parse_video_data, data["items"])))
        .assign(trending_position=lambda x: x.index.values + 1)
        .assign(region=lambda x: region)
        .assign(created_ts=[pd.Timestamp.now()] * top_n_videos)
        .set_index("video_id")
    )
    return videos_df


def get_file_path(dir: Optional[str] = None) -> str:
    """
    Return a video data file path with current date. 
    Set `dir` as a parent directories if provided.

    Return a generated path
    """
    now_ts = pd.Timestamp.now().date()
    if dir is None:
        return f"video_data_{now_ts}.csv"
    return os.path.join(dir, f"video_data_{now_ts}.csv")


def fetch_youtube_trending_data(
    regions: Iterable[str] = ("US", "GB", "RU", "DE"), save_data: bool = True
) -> pd.DataFrame:
    """
    Get data from Youtube trending page for every video on the page (50 videos) 
    for every region in `regions`.

    Return a pandas Dataframe.
    """
    videos_df = pd.concat(list(map(lambda x: compose_dataframe(x, 50), regions)))
    if save_data:
        if not os.path.exists("data"):
            os.mkdir("data")
        save_to_path = get_file_path("data")
        videos_df.to_csv(save_to_path)
        return videos_df, save_to_path
    return videos_df
