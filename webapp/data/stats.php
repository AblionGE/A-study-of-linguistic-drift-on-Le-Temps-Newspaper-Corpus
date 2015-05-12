<?php

require_once('setup.php');

$limit = 50;

if(isset($_GET['target'])){
    $target = $_GET['target'];
    $res = $pdo->query("SELECT * FROM ngrams WHERE name='$target'")->fetchAll(PDO::FETCH_ASSOC);
    if(sizeof($res) > 0){
        $stats = $res[0];
        $stats['freqs'] = array_map('intval', explode (";", $stats['freqs']));
        echo json_encode($stats);
    } else {
        echo '{}';
    }
} else {
    echo '{}';
}
