Spotify + Grammy Awards Pipeline

By: Jose David Santa Parra

Overview
This project implements a complete ETL pipeline that extracts data from two different sources: a Spotify dataset (CSV) and a Grammy Awards database (MySQL).
So, the goal of this project is to transform and merge them, load the result into a dimensional data warehouse and visualize insights through a Power BI dashboard.
The pipeline is fully orchestrated with Apache Airflow running on Docker, and the data warehouse runs on MySQL.

Data Sources

Spotify Dataset (CSV)

114,000 rows x 20 columns. 

Contains track-level audio features including danceability, energy, valence, tempo, loudness, acousticness, instrumentalness, liveness and speechiness, along with metadata like artist, album, genre, and popularity score.

Grammy Awards Dataset (MySQL)

4,810 rows x 10 columns. Contains Grammy Award winners from 1958 to 2019 across all categories. 

This dataset contains only winners, not all nominees. 

Fields include year, category, nominee, artist, and workers (producers/engineers).

EDA Findings

Spotify

The dataset is structurally clean but has some nuances worth noting. 

Around 16,000 tracks have a popularity score of 0 but these are valid entries, simply unrated or very obscure tracks, not necessarily errors.

The most interesting structural feature is that the same track_id appears in multiple rows with different genres, so i understood that 
the same song is classified under several categories. 
I took it as intentional and that it maybe reflects how Spotify's genre tagging works so it was preserved throughout the pipeline.

Audio feature distributions show that most tracks cluster around mid-range values for danceability and energy, with instrumentalness being heavily 
skewed toward zero (this can only mean that most tracks have vocals). 

Valence: Spotify's measure of musical positivity is fairly evenly distributed, suggesting the dataset covers the full emotional spectrum of music.

Grammys

The Grammy dataset has a notable structural quirk: roughly 40% of rows have a null artist field. 
This is not a data quality issue since it reflects how the Grammys credit work. In categories like Song of the Year, 
the award goes to the songwriter or producer (listed in workers), not the performing artist. 
Dropping these rows would have eliminated 40% of Grammy history, so they were preserved with artist = "N/A".

The dataset spans 62 years (1958–2019) and covers a wide range of categories.

Architecture

The two extract tasks run in parallel. 

Each clean task waits for its own extract. 

The transform task waits for both cleans to finish before merging the datasets. 

The load task runs last.

ETL Design Decisions

Extract

Spotify is read directly from CSV. 

Grammys are extracted from a MySQL database (grammys_db).

Cleaning

Spotify:

Dropped 450 fully duplicated rows (confirmed: all shared the same track_id AND track_genre, carrying zero new information).

Dropped 3 rows with nulls in core identity columns (track_id, track_name, artists, album_name).

Dropped 163 rows with time_signature = 0 because is musically impossible.

Kept rows with duplicate track_id across different genres intentionally since the same song appearing in multiple genres is a structural feature of the dataset, not a data quality issue.

Mapped key (int 0–11) to pitch class names (C, C#, D...) and mode (0/1) to Major/Minor.

Converted duration_ms to duration_s for readability.

Grammys:

Dropped img, title, published_at, and updated_at. I considered these rows to be web metadata with no analytical value. Plus, published_at had only 4 unique values for 4,810 rows, none of which corresponded to actual ceremony dates.

Filled artist nulls (1,840 rows) with "N/A" instead of dropping. Since these rows represent valid winners in songwriter/producer categories where the credited party is in the workers field.

Filled nominee nulls (6 rows) and workers nulls (2,190 rows) with "N/A" for the same reason.

Normalized category to title case to ensure consistent grouping across years.

Transform & Merge

The merge strategy is a left join from Spotify, using the primary artist as the join key. Before joining, Grammy artist names are normalized by removing collaborator markers (featuring, &, with, parentheses) and converting to lowercase, improving the match rate from ~26% to ~33% of Grammy artists found in Spotify.

Grammy data is aggregated per artist before the join instead of joining row by row which would multiply Spotify rows, each artist's Grammy history is summarized into a single row with grammy_wins, grammy_first_year, grammy_last_year, and grammy_categories. This keeps the grain clean: one row per track + genre combination.

A grammy_winner boolean flag is added to every Spotify track, making it easy to segment any analysis between Grammy-winning artists and the rest.

Dimensional Model (Star Schema)

The data warehouse follows a star schema with one fact table and three dimension tables.

dim_artist: one row per primary artist, with Grammy aggregated fields.

dim_track: one row per unique track, including categorical audio attributes (key_note, mode, time_signature) that describe the song but are not meaningful to aggregate.

dim_genre: one row per genre.

fact_streams: grain is one row per track + genre combination. 

The fact table measures are all numeric and aggregable: popularity, danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, tempo.

Surrogate keys are generated in Python using range(1, n+1).

Dashboard and KPIs & Visualizations

The Power BI dashboard connects directly to the spotify_grammys_dw MySQL database via ODBC DNS.

KPIs

Total tracks in the spotify dataset.

Total Grammy winning artists present in Spotify.

Genres with most Grammy-winning artists, shows which musical genres are most represented among Grammy winners.

Popularity vs Danceability by Grammy status, explores whether Grammy winners tend to be more or less popular and danceable on Spotify.

Average audio features, compares energy, valence, and acousticness, revealing differences in the music profile of grammy winners.

Challenges & Limitations

Artist matching is imperfect. The join between Spotify and Grammys relies on normalized text matching of artist names. 
Even after cleaning, not every artists or grammy winners in general were found in Spotify. 
This is partly because the Grammy dataset spans back to 1958 so many historical artists predate Spotify 
and partly because name formatting inconsistencies between the two sources are hard to resolve.

Google Drive integration was not implemented. 

The workshop required loading the merged dataset to Google Drive via API, I tried two different methods: 

Service accounts: the simplest authentication method, but I do not have storage quota on personal Google accounts and require Shared Drives (a Google Workspace feature). 

OAuth2 delegation is the correct alternative but adds significant setup complexity. 

This decision was made to focus effort on the pipeline, data model, and visualizations.

Memory constraints on Docker.

Running Airflow with CeleryExecutor, Redis, and PostgreSQL on a machine with limited RAM available for Docker (3.59GB) 
caused out-of-memory crashes when passing large DataFrames between tasks as JSON.

Since web and official docker solutions like creating a .wslconfig file didn't work to me. Two solutions were implemented 

One of them was to write intermediate CSVs to a shared volume and pass file paths between tasks instead of the data itself.

Another important change was made in the .yaml file for the same reason: airflow-dag-processor and airflow-triggerer were deleted from the file.

These two decisions allowed me to run the DAG with no problem and dramatically reduced memory pressure.


Setup & Deployment

Requirements

Docker Desktop (with WSL2 enabled on Windows)

MySQL 8.0 running locally

Power BI Desktop

1. Clone the repository

        git clone https://github.com/Davjos1881/etl_workshop_03.git

2. Configure environment variables

Create a .env file in the root with:

    AIRFLOW_UID=50000

3. Set up MySQL permissions

The pipeline connects from Docker to your local MySQL instance. Run this in MySQL Workbench:

    CREATE USER 'root'@'%' IDENTIFIED BY '';
    GRANT ALL PRIVILEGES ON spotify_grammys_dw.* TO 'root'@'%';
    GRANT ALL PRIVILEGES ON grammys_db.* TO 'root'@'%';
    FLUSH PRIVILEGES;

Also make sure the grammys_db database exists and has the grammys table populated from the Grammy Awards CSV.

4. Create the Data Warehouse schema

Run the DDL script in MySQL Workbench to create the spotify_grammys_dw database and its tables (dim_artist, dim_genre, dim_track, fact_streams).

5. Start Airflow

        docker-compose up -d

Wait for all services to be healthy, then open http://localhost:8080 (user: airflow, password: airflow).

9. Trigger the DAG

In the Airflow UI, find etl_spotify_grammys and trigger it manually. The pipeline runs the following tasks in order:

<img width="1111" height="289" alt="image" src="https://github.com/user-attachments/assets/55fcc6db-6f2d-45f3-8c40-ed91d8deaa5a" />


10. Connect Power BI

Use an ODBC DNS connection pointing to spotify_grammys_dw on your local MySQL instance to build the dashboard.

Important Notes

IP address changes per network. Every time you switch Wi-Fi networks, update the engine in the etl_pipeline.py file: 

    create_engine(
            "mysql+pymysql://root:@<YOUR_CURRENT_IP>:3306/spotify_grammys_dw"
        ).

You may also change the base path on the main.py, (this isn't really neccesary but if you want to run the pipeline without Airflow, you can do it)

    base_path = Path(r"C:\YOUR_PATH\workshop_02")
