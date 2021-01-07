<?php

namespace gunter\train\bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\Core\Iterator\ItemIterator;

/**
 * 練習用 主要函式庫
 *
 * @package gunter\train\bigquery
 */
class trainClass
{
    /**
     * Get BigQuery
     *
     * @return BigQueryClient
     */
    protected function _getClient()
    {
        static $bigQueryClient = new BigQueryClient([
            'keyFilePath' => defined('GCP_CREDENTIAL_PATH') ? GCP_CREDENTIAL_PATH : null,
            'location' => 'asia-east1',
        ]);

        return $bigQueryClient;
    }

    /**
     * Conv QueryResults to Array
     *
     * @param  ItemIterator      $results
     * @throws GoogleException
     * @return array
     */
    protected function _toArray(ItemIterator $results)
    {
        $results = [];
        foreach ($results as $row) {
            $results[] = $row;
        }
        return $results;
    }

    /**
     * Print query results
     *
     * @param  QueryResults      $queryResults
     * @throws GoogleException
     * @return void
     */
    protected function _output(QueryResults $queryResults)
    {
        $results = $this->_toArray($queryResults->rows());

        foreach ($results as $idx => $row) {
            printf('--- Row %s ---' . PHP_EOL, $idx);
            foreach ($row as $column => $value) {
                printf('%s: %s' . PHP_EOL, $column, $value);
            }
        }

        printf('Found %s row(s)' . PHP_EOL, count($results));
    }
}
