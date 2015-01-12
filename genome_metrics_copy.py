import mysql_connect2 as m
import calculate as c
# import google_docs_downloader as g

class GenomeMetrics:

    def __init__(self):
        self.connection = m.ConnectMySQL('rds_master', 'rds_reader', 'rds_master')
        self.calculator = c.Calculate()
        self.get_tag_performance = TagPerformance(self.connection)
        self.update_download_trends = PlayStoreDownloadTrends(self.connection)

    def update_editor_genome_metrics(self):
        current_day = self.calculator.get_current_serverday(self.connection)
        yesterday = current_day - 1
        last_update_day = self.calculator.get_last_updated_day_plus_one(self.connection, 'editor_tags')

        self.get_what_the_editors_tagged(last_update_day, current_day)
        self.get_tag_performance.update_tag_performance()
        self.update_download_trends.update_playstore_download_trends(yesterday)
        # self.get_tag_data_from_google()
        # self.update_editor_tags_table_with_google_and_metrics_data(last_update_day)
        # self.update_genome_metric_reports(last_update_day, current_day) 
        self.connection.close()

    def get_what_the_editors_tagged(self, last_update_day, current_day):
        while last_update_day < current_day:
            date = self.calculator.serverday2date(last_update_day)
            self.update_editor_tags_table(last_update_day, date)
            self.update_editor_tag_report_table(date)
            last_update_day += 1

    def update_editor_tags_table_with_google_and_metrics_data(self, last_update_day):
        self.update_PIE_tags_in_editor_tags_table(last_update_day)
        self.update_in_playstore_status_in_editor_tags_table()

    def update_genome_metric_reports(self, last_update_day, current_day):
        while last_update_day < current_day:
            date = self.calculator.serverday2date(last_update_day)
            self.update_total_tag_metrics(last_update_day, date)
            self.update_editor_tag_metrics(last_update_day, date)
            self.update_PIE_tag_metrics(last_update_day, date)
            # self.update_new_tag_metrics(last_update_day, date)
            # self.update_hot_tag_metrics(last_update_day, date)
            last_update_day += 1

    def update_editor_tags_table(self, server_day, date):

        day = self.calculator.day2dayofweek(server_day)
        print('Getting apps and games tagged for: ' + str(date))
        editor_tags_query = \
        '''
        INSERT INTO stats.editor_tags 
        (server_day, 
        `date`, 
        `day`, 
        in_playstore, 
        app_type, 
        package_name, 
        play_category, 
        internal_rating, 
        play_rating, 
        size, 
        price, 
        min_downloads, 
        number_of_tags, 
        tagged_by)
            SELECT 
            ''' + str(server_day) + ''' AS 'server_day', 
            "''' + date + '''" AS 'date', 
            "''' + day + '''" AS 'day', 
            1 AS 'in_playstore',
            a.apptype AS 'app_type', 
            a.packagename AS 'package_name', 
            UPPER(a.category) AS 'category', 
            FORMAT(a.internalrating,1) AS 'internal_rating', 
            FORMAT((a.rating*2),1) AS 'play_rating', 
            a.size AS 'size', 
            a.price AS 'price', 
            CAST(replace(SUBSTRING_INDEX(a.downloadcounttext,'-', 1),',','') AS UNSIGNED) AS 'min_downloads', 
            COUNT(DISTINCT(node_id)) AS 'number_of_tags',
            updated_by AS 'tagged_by'
            
            FROM apps.apps AS a, apps.tlink AS t
            WHERE t.target_id = a.id
                AND DATE(update_date) = "''' + date + '''"
                AND updated_by NOT LIKE ('ALGO_%')
                AND updated_by IS NOT NULL
                AND packagename NOT IN (SELECT package_name FROM stats.editor_tags)
            GROUP BY package_name
            HAVING 
                (app_type = 'APPLICATION'
                AND number_of_tags >= 3
                AND internal_rating > 0)
                    OR 
                (app_type = 'GAME'
                AND number_of_tags >= 12
                AND internal_rating > 0)
        '''
        self.connection.execute_query1(editor_tags_query, False)

    def update_editor_tag_report_table(self, date):
        editor_tag_report_query = \
        '''
        INSERT INTO editor_tag_report (server_day, `date`, `day`, tagged_by, total_tags, apps_tagged, games_tagged, rating_1to6, rating_7to8, rating_9to10)        
        SELECT server_day, `date`, `day`, tagged_by,
        COUNT(CASE WHEN (`date` = "''' + date + '''") THEN 1 ELSE NULL END) AS total_tags,
        COUNT(CASE WHEN (app_type = 'APPLICATION') THEN app_type ELSE NULL END) AS apps_tagged, 
        COUNT(CASE WHEN (app_type = 'GAME') THEN app_type ELSE NULL END) AS games_tagged, 
        COUNT(CASE WHEN (internal_rating < 7) THEN internal_rating ELSE NULL END) AS rating_1to6,
        COUNT(CASE WHEN (internal_rating >= 7 AND internal_rating <= 8) THEN internal_rating ELSE NULL END) AS rating_7to8,
        COUNT(CASE WHEN (internal_rating >= 9) THEN internal_rating ELSE NULL END) AS rating_9to10
            FROM editor_tags
            WHERE `date` = "''' + date + '''"
            GROUP BY tagged_by
            HAVING total_tags > 0;
        '''
        self.connection.execute_query1(editor_tag_report_query, False)

    def get_tag_data_from_google(self):
        get_tag_data_from_google = g.UploadGoogleSpreadSheets(self.connection)
        editor_tag_qa = g.upload_params('editor_tag_qa')
        editor_tag_queue = g.upload_params('editor_tag_queue')
        table_list = [editor_tag_qa, editor_tag_queue]
        multi_upload = [editor_tag_queue]
        get_tag_data_from_google.build_dictionary_and_upload_to_DB(table_list, multi_upload, False)

    def update_PIE_tags_in_editor_tags_table(self, last_update_day):
        update_pie_tags_query = \
        '''
        UPDATE stats.editor_tags AS e, stats.editor_tag_queue AS q
        SET e.PIE = 1
        WHERE e.package_name = q.package_name
        AND e.server_day >= ''' + str(last_update_day) + '''
        '''
        self.connection.execute_query1(update_pie_tags_query, False)

    def update_in_playstore_status_in_editor_tags_table(self):
        update_in_playstore_status_query = \
        '''
        UPDATE stats.editor_tags AS e, apps.apps AS a
        SET in_playstore = 0
        WHERE a.status = 2
        AND e.package_name = a.packagename
        AND e.status != 0
        '''
        self.connection.execute_query1(update_in_playstore_status_query, False)

    def update_hot_tags_in_editor_tags_table(self, day_to_query):
        get_hot_tags_query = \
        '''
        SELECT 
        FROM stats.playstore_download_trends
        '''

    def get_publication_date_for_editor_tags_table(self):
        print('p')

    def update_total_tag_metrics(self, server_day, date):
        total_tags_metrics_query = \
        '''
        INSERT INTO stats.editor_genome_metrics
        SELECT
        ''' + str(server_day) + ''' AS 'server_day',
        ''' + date + ''' AS 'date',
        'Total' AS 'tag_segment',
        avg(play_rating) AS 'avg_rating',
        count(distinct(package_name)) AS 'tag_count',
        sum(7day_impression) AS '7day_impression',
        sum(7day_install) AS '7day_install'
        FROM stats.editor_tags
        WHERE server_day = ''' + str(server_day)
        self.connection.execute_query1(editor_tag_report_query, False)

    def update_editor_tag_metrics(self, server_day, date):
        editor_tag_metrics_query = \
        '''
        INSERT INTO stats.editor_genome_metrics
        SELECT
        ''' + str(server_day) + ''' AS 'server_day',
        ''' + date + ''' AS 'date',
        'Editor' AS 'tag_segment',
        avg(play_rating) AS 'avg_rating',
        count(distinct(package_name)) AS 'tag_count',
        sum(7day_impression) AS '7day_impression',
        sum(7day_install) AS '7day_install'
        FROM stats.editor_tags
        WHERE server_day = ''' + str(server_day) + '''
        AND `PIE` = 0
        '''
        self.connection.execute_query1(editor_tag_metrics_query, False)

    def update_PIE_tag_metrics(self, server_day, date):
        PIE_tag_metrics_query = \
        '''
        INSERT INTO stats.editor_genome_metrics
        SELECT
        ''' + str(server_day) + ''' AS 'server_day',
        ''' + date + ''' AS 'date',
        'PIE' AS 'tag_segment',
        avg(play_rating) AS 'avg_rating',
        count(distinct(package_name)) AS 'tag_count',
        sum(7day_impression) AS '7day_impression',
        sum(7day_install) AS '7day_install'
        FROM stats.editor_tags
        WHERE server_day = ''' + str(server_day) + '''
        AND `PIE` = 1
        '''
        self.connection.execute_query1(PIE_tag_metrics_query, False)

    def update_new_tag_metrics(self, server_day, date):
        new_tag_metrics_query = \
        '''
        INSERT INTO stats.editor_genome_metrics
        SELECT
        ''' + str(server_day) + ''' AS 'server_day',
        ''' + date + ''' AS 'date',
        'New' AS 'tag_segment',
        avg(play_rating) AS 'avg_rating',
        count(distinct(package_name)) AS 'tag_count',
        sum(7day_impression) AS '7day_impression',
        sum(7day_install) AS '7day_install'
        FROM stats.editor_tags
        WHERE server_day = ''' + str(server_day) + '''
        AND DATEDIFF(`date`, publication_date) BETWEEN 0 AND 14
        '''
        self.connection.execute_query1(new_tag_metrics_query, False)

    def update_hidden_tag_metrics(self, server_day, date):
        hidden_tag_metrics_query = \
        '''
        INSERT INTO stats.editor_genome_metrics
        SELECT
        ''' + str(server_day) + ''' AS 'server_day',
        ''' + date + ''' AS 'date',
        'New' AS 'tag_segment',
        avg(play_rating) AS 'avg_rating',
        count(distinct(package_name)) AS 'tag_count',
        sum(7day_impression) AS '7day_impression',
        sum(7day_install) AS '7day_install'
        FROM stats.editor_tags
        WHERE server_day = ''' + str(server_day) + '''
        AND DATEDIFF(`date`, publication_date) > 14
        AND min_downloads < 1000
        '''
        self.connection.execute_query1(hidden_tag_metrics_query, False)

    def update_hot_tag_metrics(self, server_day, date):
        hot_tag_metrics_query = \
        '''
        INSERT INTO stats.editor_genome_metrics
        SELECT
        ''' + str(server_day) + ''' AS 'server_day',
        ''' + date + ''' AS 'date',
        'New' AS 'tag_segment',
        avg(play_rating) AS 'avg_rating',
        count(distinct(package_name)) AS 'tag_count',
        sum(7day_impression) AS '7day_impression',
        sum(7day_install) AS '7day_install'
        FROM stats.editor_tags
        WHERE server_day = ''' + str(server_day) + '''
        AND hot = 1
        '''
        self.connection.execute_query1(hot_tag_metrics_query, False)

#########################################################################################################
#########################################################################################################

class TagPerformance:

    def __init__(self, connection):
        self.connection = connection
        self.calculator = c.Calculate()
        self.update_tag_performance()

    def update_tag_performance(self):
        self.create_temp_tables()
        min_serverday, max_serverday = self.get_serverdays()
        while min_serverday <= max_serverday:
            print('Getting tag performance for: ' + str(min_serverday))
            tagged_list = self.get_tagged_apps(min_serverday)
            day_to_query = min_serverday + 1

            for count in range(7):
                self.get_tag_performance_for_one_day(tagged_list, day_to_query)
                day_to_query += 1

            self.sum_days_for_tag_performance(min_serverday)
            self.insert_into_editor_tags_table()
            self.reset_tables_for_next_day
            min_serverday += 1

    def create_temp_tables(self):
        create_temp_app_performance_table1 = \
        '''CREATE TABLE app_performance_temp (
        package_name VARCHAR(255),
        impression INT(10) default 0,
        detail INT(10) default 0,
        market INT(10) default 0,
        `install` INT(10) default 0,
        `uninstall` INT(10) default 0)'''
        create_temp_app_performance_table2 = \
        '''CREATE TABLE app_performance_sums (
        server_day SMALLINT(6),
        package_name VARCHAR(255),
        impression INT(10),
        detail INT(10),
        market INT(10),
        `install` INT(10),
        `uninstall` INT(10),
        PRIMARY KEY (server_day, package_name))'''
        self.connection.execute_query1(create_temp_app_performance_table1, False)
        self.connection.execute_query1(create_temp_app_performance_table2, False)

    def get_serverdays(self):
        min_day_query = \
        '''
        SELECT min(server_day) FROM
        (SELECT server_day, sum(7day_impression) AS 'impression'
        FROM stats.editor_tags WHERE server_day >= 5285
        GROUP BY server_day
        HAVING 'impression' = 0) AS i
        WHERE impression = 0
        '''
        result, cursor = self.connection.execute_query1(min_day_query, True)
        min_serverday = (result[0])[0]
        cursor.close()

        current_day = self.calculator.get_current_serverday(self.connection)
        max_serverday = current_day - 9

        return min_serverday, max_serverday

    def get_tagged_apps(self, tag_performance_day):

        get_tagged_apps_query = \
            '''SELECT package_name FROM stats.editor_tags WHERE server_day = ''' \
                + str(tag_performance_day) 
        tagged_apps, cursor = self.connection.execute_query2(get_tagged_apps_query, True)
        cursor.close()
        tagged_list = []
        for items in tagged_apps:
            for package_names in items:
                tagged_list.append(package_names)
        return tagged_list

    def get_tag_performance_for_one_day(self, tagged_list, day_to_query):
        for package_names in tagged_list:
            app_performance_query = self.app_performance_query(day_to_query, package_names)
            result, cursor = self.connection.execute_query2(app_performance_query, True, False)
            insert_statement = self.connection.prepare_results('app_performance_temp', cursor)
            cursor.close()
            self.connection.write_update(insert_statement, result)

    def app_performance_query(self, server_day, package_name):
        day = server_day
        tracking_week = self.calculator.day2week(server_day)
        app = package_name

        app_performance_query = \
        ''' 
        SELECT
        app AS 'package_name', 
        sum(impression) AS 'impression', 
        sum(detail) AS 'detail', 
        sum(market) AS 'market', 
        sum(`install`) AS 'install',
        sum(samedayuninstall) AS 'uninstall'
        FROM ''' + tracking_week + '''
        WHERE `day` = ''' + str(day) + '''
        AND app = "''' + app + '''"'''
        return app_performance_query

    def sum_days_for_tag_performance(self, tag_performance_day):
        app_performance_query2 = \
        '''
        INSERT INTO app_performance_sums 
        SELECT 
        ''' + str(tag_performance_day) + ''',
        package_name, 
        IF (sum(impression) IS NULL, 0, sum(impression)), 
        IF (sum(detail) IS NULL, 0, sum(detail)),
        IF (sum(market) IS NULL, 0, sum(market)),
        IF (sum(`install`) IS NULL, 0, sum(`install`)),
        IF (sum(`uninstall`) IS NULL, 0, sum(`uninstall`))
        FROM app_performance_temp
        GROUP BY package_name'''
        self.connection.execute_query1(app_performance_query2, False, False)

    def insert_into_editor_tags_table(self):
        editor_tags_insert_query = \
        '''
        INSERT INTO stats.editor_tags 
        (server_day, package_name, 7day_impression, 7day_detail, 7day_market, 7day_install, 
            7day_uninstall)
        SELECT s.server_day, s.package_name, s.impression, s.detail, s.market, s.install, 
        s.uninstall
            FROM stats.app_performance_sums AS s, stats.editor_tags AS e
            WHERE s.server_day = e.server_day
            AND s.package_name = e.package_name
        ON DUPLICATE KEY UPDATE 7day_impression = s.impression, 7day_detail = s.detail, 
        7day_market = s.market, 7day_install = s.install, 7day_uninstall = s.uninstall
        '''
        self.connection.execute_query1(editor_tags_insert_query, False, False)

    def reset_tables_for_next_day(self):
        truncate_app_performance_temp = "TRUNCATE app_performance_temp"
        truncate_app_performance_sums = "TRUNCATE app_performance_sums"
        self.connection.execute_query1(truncate_app_performance_temp, False, False)
        self.connection.execute_query1(truncate_app_performance_sums, False, False)

#########################################################################################################
#########################################################################################################

class PlayStoreDownloadTrends:

    def __init__(self, connection):
        self.connection = connection
        self.calculator = c.Calculate()

    def update_playstore_download_trends(self, day_to_query):
        self.create_table_of_downloadcounts(day_to_query)
        self.add_and_remove_apps_from_playstore_download_trends_table()
        self.update_download_counts_in_playstore_download_trends_table()
        self.connection.close()

    def create_table_of_downloadcounts(self, day_to_query):
        create_table_of_downloadcounts_query = \
        '''
        CREATE TEMPORARY TABLE download_counts
        SELECT
        ''' + str(day_to_query) + ''' AS 'server_day',
        packagename AS 'package_name',
        CAST(replace(SUBSTRING_INDEX(downloadcounttext,'-', 1),',','') AS UNSIGNED) AS 'downloads'
        FROM apps.apps
        WHERE `status` != 2
        '''
        self.connection.execute_query1(create_table_of_downloadcounts_query, False)

    def add_and_remove_apps_from_playstore_download_trends_table(self):
        remove_apps_from_playstore_download_trends_table_query = \
        '''
        DELETE FROM stats.playstore_download_trends
        WHERE package_name IN (SELECT packagename FROM apps.apps WHERE `status` = 2)
        '''
        #self.connection.execute_query1(remove_apps_from_playstore_download_trends_table_query, False)

        add_apps_to_playstore_download_trends_query = \
        '''
        INSERT INTO stats.playstore_download_trends (package_name)
        SELECT package_name FROM download_counts
        WHERE package_name NOT IN (SELECT package_name FROM playstore_download_trends)
        '''
        self.connection.execute_query1(add_apps_to_playstore_download_trends_query, False)

    def update_download_counts_in_playstore_download_trends_table(self):
        download_count_range = [500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000]
        for counts in download_count_range:
            self.download_counts_updater(counts)

    def download_counts_updater(self, download_count):
        update_download_counts_query = \
        '''
        UPDATE playstore_download_trends AS p, download_counts AS d
        SET p.''' + str(download_count) + ''' = d.server_day
        WHERE p.package_name = d.package_name
        AND d.downloads = ''' + str(download_count) + '''
        AND p.''' + str(download_count) + ''' = 0
        '''
        self.connection.execute_query1(update_download_counts_query, False)



# update = GenomeMetrics()
# update.update_editor_genome_metrics()

print 'start'
connection = m.ConnectMySQL('rds_heavy','rds_heavy','rds_master')
# TagPerformance(connection)
# t.create_temp_tables()
# t.reset_tables_for_next_day()
print 'done'