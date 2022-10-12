import io
import requests
import pandas
from bs4 import BeautifulSoup

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"


def find_filenames_bs4(url):
    with requests.get(url) as response:
        table = BeautifulSoup(response.content, "html.parser").find("table")
        rows = table.find_all("tr")
        url_list = [
            f"{url}{f.previous_sibling.string}"
            for td in rows
            if (f := td.find("td", string="2022-02-07 14:03  "))
        ]
        return url_list

def download_and_print(url_list):
    for e in url_list:
        res = requests.get(e)

        df = pandas.read_csv(io.BytesIO(res.content))
        print(df.loc[df.HourlyDryBulbTemperature.idxmax()])


def main():
    
    url_list = find_filenames_bs4(url)
    download_and_print(url_list)


if __name__ == "__main__":
    main()