import os

import schedule
import warnings
from trendscrap.api_helper import upload_file, exists
from trendscrap.data_fetch import fetch_youtube_trending_data, get_file_path


def daily_routine():
    """
    Fetch data, upload to the Google Drive.
    """
    videos_df, video_data_path = fetch_youtube_trending_data(save_data=True)
    n_regions = 4  # default
    if videos_df.shape[0] != n_regions * 50:
        warnings.warn("Number of videos is lesser than expected")
        return

    upload_file(
        video_data_path, f"data/youtube_trending_explorer/{get_file_path()}",
    )


def double_check():
    """
    Rerun daily routine if file hasn't been uploaded to the Drive.
    """
    file_name = get_file_path()
    if not exists(file_name):
        daily_routine()


def main():
    schedule.every().day.at("08:00").do(daily_routine)
    schedule.every().day.at("11:00").do(double_check)
    schedule.every().day.at("14:00").do(double_check)
    schedule.every().day.at("17:00").do(double_check)
    schedule.every().day.at("20:00").do(double_check)
    while True:
        schedule.run_pending()


if __name__ == "__main__":
    main()
