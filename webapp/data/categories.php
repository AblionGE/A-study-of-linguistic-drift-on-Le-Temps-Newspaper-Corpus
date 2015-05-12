<?php

require_once('setup.php');

$limit = 50;

if(isset($_GET['id'])){
    $id = $_GET['id'];
    $categories = $pdo->query("SELECT * from categories WHERE entries_id = $id");
    echo json_encode($categories->fetchAll(PDO::FETCH_ASSOC));
} else {
    echo '{}';
}
