<?php

namespace gunter\train\bigquery;

use Exception;
use Google\Cloud\Core\Exception\GoogleException;
use Google\Cloud\Core\ExponentialBackoff;
use InvalidArgumentException;

/**
 * 練習二 建立 異步 & 同步 查詢工作
 *
 * @package gunter\train\bigquery
 */
class class2 extends trainClass
{
    /**
     * Query string
     *
     * @var string
     */
    public $queryStr = 'SELECT `string_field_1` FROM `bqtest.A` LIMIT 3';

    /**
     * Asynchronous run query job
     *
     * @throws Exception
     * @throws InvalidArgumentException
     * @throws GoogleException
     * @return void
     */
    public function asyncRun()
    {
        // Get BigQuery client API
        $bigQuery = $this->_getClient();

        // 建立查詢工作
        $jobConfig = $bigQuery->query($this->queryStr);

        // 異步執行工作
        $job = $bigQuery->startJob($jobConfig);

        // 詢問工作進度，最多詢問 10 次
        $backoff = new ExponentialBackoff(10);

        // 建立重複嘗試任務
        $backoff->execute(function () use ($job) {
            print('Waiting for job to complete' . PHP_EOL);

            // 重新取得工作狀態
            $job->reload();

            // 檢查工作狀態是否完成
            if (!$job->isComplete()) {
                /*
                 * 未完成時會丟出例外，並隨機休息 1 ~ 6 秒
                 * @see ExponentialBackoff::calculateDelay()
                 */
                throw new Exception('Job has not yet completed', 500);
            }
        });

        // 取得查詢結果
        $queryResults = $job->queryResults();

        // 輸出查詢結果
        $this->_output($queryResults);
    }

    /**
     * Synchronous run query job
     *
     * @throws InvalidArgumentException
     * @throws Exception
     * @throws GoogleException
     * @return void
     */
    public function syncRun()
    {
        // Get BigQuery client API
        $bigQuery = $this->_getClient();

        // 建立查詢工作
        $jobConfig = $bigQuery->query($this->queryStr);

        // 同步執行工作
        $job = $bigQuery->runJob($jobConfig);

        // 取得查詢結果
        $queryResults = $job->queryResults();

        // 輸出查詢結果
        $this->_output($queryResults);
    }
}
