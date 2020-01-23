import schedule
import os

from ytexplorer.data_fetch import fetch_youtube_trending_data
from ytexplorer.api_helper import build_drive_service, upload_file


def daily_routine():
    videos_df, video_data_path = fetch_youtube_trending_data(save_data=True)
    drive_service = build_drive_service()
    upload_file(video_data_path, f'data/youtube_trending_explorer/{os.path.split(video_data_path)[-1]}', drive_service)
    
    
def main():
    schedule.every().day.at("11:00").do(daily_routine)
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
