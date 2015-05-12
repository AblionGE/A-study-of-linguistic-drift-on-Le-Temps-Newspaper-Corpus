<?php

require_once('setup.php');

$limit = 50;

if(isset($_GET['like'])){
    $target = $_GET['like'];
    $stats = $pdo->query("SELECT synonyms FROM entries WHERE name='$target'")->fetchAll(PDO::FETCH_ASSOC)[0];
    $words = explode (";", $stats['synonyms']);
    echo json_encode($words);
} else {
    echo '{}';
}
