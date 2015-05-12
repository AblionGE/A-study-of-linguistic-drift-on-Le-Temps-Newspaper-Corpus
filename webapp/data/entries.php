<?php

require_once('setup.php');

$limit = 50;

if(isset($_GET['like'])){
    $prefix = $_GET['like'];
    $entities = $pdo->query("SELECT id,name,display from entries WHERE name LIKE '$prefix%' LIMIT $limit");
    echo json_encode($entities->fetchAll(PDO::FETCH_ASSOC));
} else {
    echo '[]';
}
