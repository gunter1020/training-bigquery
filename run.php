<?php

require 'vendor/autoload.php';

use Dotenv\Dotenv;
use gunter\train\bigquery\class1;

/**
 * Set root path
 */
$rootPath = __DIR__;

/**
 * Init environment
 */
$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->safeLoad();
$dotenv->required('GCP_CREDENTIAL');

/**
 * defined variable
 */
defined('GCP_CREDENTIAL_PATH') || define('GCP_CREDENTIAL_PATH', realpath(implode(DIRECTORY_SEPARATOR, [$rootPath, $_ENV['GCP_CREDENTIAL']])));

// 練習一
$class1 = new class1();
// $class1->loadLocal('file.csv');
// $class1->loadGCS('gs://bucket/table.csv');
