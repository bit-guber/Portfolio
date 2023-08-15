# Crunchyroll ( Anime Streaming Platform ) Meta-Data Data Pipeline 
Before You going through my Project, Please visit Crunchyroll [here](https://www.crunchyroll.com/) and learn about it.<br>

You can get Extracted and transformed dataset on kaggle [here](  )

## 
![](images/ETL.png)
<br>
This project contains three script
- Gathering information from sources ( Data_Extraction.py )
- Wide Spread unstructured files transform to Analysis Friendly format ( Data_transformation.py )
- Initial Exploratory data analysis (EDA) from Extracted data ( Basic_Analsis.ipynb )<br><br>

This project is fully written Python 
#### I used tools are 
- undetected_chrome ( disguise scraping bot as genuine client ) 
- requests ( make API requests and managing session  )
- pandas ( Data Manipulations tool )
- Seaborn ( visualization tool )

### How it Works
#### Extraction
There are few steps to follows <br>
Step 1 - Disguise as Genuine client brower <br>
Step 2 - Gather Session cookies <br>
Step 3 - Make Sequential Requests to server <br>
Step 4 - when Cookies got expired repeat step 1 <br>
Step 5 - Until end of requests list

#### Transformation
There Some sanity check go through all of extracted data like 🧐 <br>
>Is feature has only a unique value occur all response data ?<br>
Any duplicate feature present on Response data ?<br>
Avoid Null Features include Dataset

#### Analysis 
I only manage get few interesting questions...
<br><br>Feel Free to give me questions on linkedin [here]( www.linkedin.com/in/guber-mani-894b34227 )
