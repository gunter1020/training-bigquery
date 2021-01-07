<?php

namespace gunter\train\bigquery;

use Exception;
use Google\Cloud\BigQuery\Table;
use InvalidArgumentException;

/**
 * 練習一 建立 資料表
 *
 * @package gunter\train\bigquery
 */
class class1 extends trainClass
{
    /**
     * 載入本地 CSV 檔案

     * @param  string                   $filePath
     * @throws InvalidArgumentException
     * @throws Exception
     * @return void
     */
    public function loadLocal(string $filePath)
    {
        // 建立 資料集 與 資料表
        $table = $this->_createTable('train_20210107', 'class1');

        // 建立工作 - 載入本地 CSV 檔案
        $loadJobConfig = $table
            ->load(fopen($filePath, 'r'))
            ->autodetect(true)
            ->skipLeadingRows(1);

        // Get BigQuery client API
        $bigQuery = $this->_getClient();

        // 執行工作
        $bigQuery->runJob($loadJobConfig);
    }

    /**
     * 載入 GCS bucket 檔案
     *
     * @param  string $gcsPath
     * @return void
     */
    public function loadGCS(string $gcsPath)
    {
        // 建立 資料集 與 資料表
        $table = $this->_createTable('train_20210107', 'class1');

        // Get BigQuery client API
        $bigQuery = $this->_getClient();

        // 將 GCS bucket 中的檔案載入至目標資料表
        $bigQuery->load()
            ->destinationTable($table)
            ->sourceUris([$gcsPath]);
    }

    /**
     * 建立 資料集 與 資料表
     *
     * @param  string  $datasetName
     * @param  string  $tableName
     * @return Table
     */
    private function _createTable(string $datasetName, string $tableName)
    {
        // Get BigQuery client API
        $bigQuery = $this->_getClient();

        // 建立資料集
        $dataset = $bigQuery->dataset($datasetName)->exists()
            ? $bigQuery->dataset($datasetName)
            : $bigQuery->createDataset($datasetName);

        // 建立資料表
        $table = $dataset->table($tableName)->exists()
            ? $dataset->table($tableName)
            : $dataset->createTable($tableName);

        return $table;
    }
}
