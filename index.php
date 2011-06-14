<?php

require 'Cassandra.php';

$servers = array(
	array(
		'host' => '127.0.0.1',
		'port' => 9160
	)
);

$s = microtime(true);
$c = Cassandra::createInstance($servers);
out('constructing: '.round((microtime(true) - $s) * 1000, 4).' ms');

$s = microtime(true);
out('version: '.Cassandra::getInstance()->getVersion());
out('getting version: '.round((microtime(true) - $s) * 1000, 4).' ms');

$c->useKeyspace('BlogSharder');

//var_dump($c->describeKeyspace());
//var_dump($c->getKeyspaceSchema());

out('user-1'."\n".print_r($c->cf('shard')->get('user-1'), true));
out('user-1:group,properties'."\n".print_r($c->cf('shard')->get('user-1', array('group', 'properties')), true));
out('user-1:group-properties'."\n".print_r($c->cf('shard')->get('user-1', null, 'group', 'properties'), true));

out('shard.user-1'."\n".print_r($c->get('shard.user-1'), true));
out('shard.user-1:group,properties'."\n".print_r($c->get('shard.user-1:group,properties'), true));
out('shard.user-1:group-properties'."\n".print_r($c->get('shard.user-1:group-properties'), true));
out('shard.user-1:group-properties|2'."\n".print_r($c->get('shard.user-1:group-properties|2'), true));
out('shard.user-1:group-properties|R'."\n".print_r($c->get('shard.user-1:properties-group|R'), true));
out('shard.user-1:group-properties|2R'."\n".print_r($c->get('shard.user-1:properties-group|2R'), true));

/*
family.key:col1,col2,coln
family.key:col1-col2
family.key:col1-col2|100
family\.name:key\:name:col\.1,col\|2|100
*/
//var_dump($c->cf('shard')->get('user-1'));

function out($text) {
	echo '<pre>'.$text.'</pre>'."\n";
}