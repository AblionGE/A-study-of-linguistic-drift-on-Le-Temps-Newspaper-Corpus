<?php

$database = array(
    'dsn' =>  'mysql:host=localhost;dbname=bigdata_thes',
    'user' => 'itbeautyadmin',
    'password' => '1234'
);

try {
    $pdo = new PDO($database['dsn'], $database['user'], $database['password'], array(PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8;'));
} catch (PDOException $e) {
    echo 'Connection failed: ' . $e->getMessage();
}
header('Content-Type: text/html; charset=utf-8');