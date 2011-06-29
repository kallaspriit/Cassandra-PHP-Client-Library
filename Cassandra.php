<?php
/**
 * Cassandra-PHP-Client.
 * 
 * Cassandra PHP-based client library for managing and querying your Cassandra
 * cluster. It's a high-level library performing all the rather complex
 * low-level lifting and providing a simple to learn and use interface.
 * 
 * Includes ideas and code snippets from PHPCassa project.
 * 
 * @author Priit Kallas <kallaspriit@gmail.com>
 */

// set the globals that the thrift library uses
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__) . '/thrift/';
define('THRIFT_PATH', $GLOBALS['THRIFT_ROOT']);

// require thrift packages
require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';

/**
 * Represents a connection to a single Cassandra node.
 * 
 * Provides direct access to the low-level Cassandra client.
 */
class CassandraConnection {
	
	/**
	 * Hostname or IP of the node.
	 * 
	 * @var string
	 */
	protected $host;
	
	/**
	 * Port of the instance, defaults to 9160.
	 * 
	 * @var integer 
	 */
	protected $port;
	
	/**
	 * Should framed or buffered transport be used.
	 * 
	 * @var boolean
	 */
	protected $useFramedTransport;
	
	/**
	 * Timeout of send operations in milliseconds.
	 * 
	 * @var integer
	 */
	protected $sendTimeoutMs;
	
	/**
	 * Timeout of receive operations in milliseconds.
	 * 
	 * @var integer
	 */
	protected $receiveTimeoutMs;
	
	/**
	 * Socket to the node.
	 * 
	 * @var TSocket
	 */
	protected $socket;
	
	/**
	 * Transport method.
	 * 
	 * @var TTransport
	 */
	protected $transport;
	
	/**
	 * Used communication protocol.
	 * 
	 * @var TBinaryProtocolAccelerated
	 */
	protected $protocol;
	
	/**
	 * The low-level cassandra client.
	 * 
	 * @var CassandraClient
	 */
	protected $client;
	
	/**
	 * Is the connection currently open.
	 * 
	 * @var boolean
	 */
	protected $isOpen;
	
	/**
	 * Constructs the connection, setting access parameters.
	 * 
	 * @param string $host Hostname or IP of the node
	 * @param integer $port Port of the instance
	 * @param boolean $useFramedTransport Use framed or buffered transport
	 * @param integer $sendTimeoutMs Timeout of send operations in milliseconds
	 * @param integer $receiveTimeoutMs Timeout of receive operations
	 */
	public function __construct(
		$host = '127.0.0.1',
		$port = 9160,
		$useFramedTransport = true,
		$sendTimeoutMs = null,
		$receiveTimeoutMs = null
	) {
		$this->host = $host;
		$this->port = $port;
		$this->useFramedTransport = $useFramedTransport;
		$this->sendTimeoutMs = $sendTimeoutMs;
		$this->receiveTimeoutMs = $receiveTimeoutMs;
		$this->isOpen = false;
		
		$this->socket = $this->createSocket(
			$host,
			$port,
			$sendTimeoutMs,
			$receiveTimeoutMs
		);
		
		if ($useFramedTransport) {
			$this->transport = $this->createFramedTransport($this->socket);
		} else {
			$this->transport = $this->createBufferedTransport($this->socket);
		}
		
		$this->transport->open();
		$this->isOpen = true;
		
		$this->protocol = new TBinaryProtocolAccelerated($this->transport);
		$this->client = new CassandraClient($this->protocol);
	}
	
	/**
	 * Closes the connection on destruction.
	 */
	public function __destruct() {
		$this->close();
	}
	
	/**
	 * Forces the connection to close.
	 * 
	 * Generally there's no need to call it yourself as it will be closed on
	 * termination.
	 */
	public function close() {
		if ($this->isOpen) {
			$this->transport->flush();
			$this->transport->close();
			
			$this->isOpen = false;
		}
	}
	
	/**
	 * Is the connection open.
	 * 
	 * @return boolean 
	 */
	public function isOpen() {
		return $this->isOpen;
	}
	
	/**
	 * Returns the low-level Cassandra client used by the wrapper.
	 * 
	 * @return CassandraClient
	 */
	public function getClient() {
		if (!$this->isOpen) {
			throw new CassandraConnectionClosedException(
				'The connection has been closed'
			);
		}
		
		return $this->client;
	}
	
	/**
	 * Returns the used transport method.
	 * 
	 * @return TTransport
	 */
	public function getTransport() {
		return $this->transport;
	}
	
	/**
	 * Returns the used transportation protocol.
	 * 
	 * @return TBinaryProtocolAccelerated
	 */
	public function getProtocol() {
		return $this->transport;
	}
	
	/**
	 * Attempts to start using given keyspace.
	 * 
	 * Using the keyspace is attempted three times before giving up.
	 * 
	 * @param string $keyspace Name of the keyspace
	 * @param string $username Optional username in case authentication is used
	 * @param string $password Optional password
	 */
	public function useKeyspace(
		$keyspace,
		$username = null,
		$password = null
	) {
		$attempts = 3;
		$success = false;

		while($attempts-- > 0 && !$success) {
			try {
				$this->client->set_keyspace($keyspace);
				$success = true;
			} catch (cassandra_InvalidRequestException $e) {
				continue;
			}
		}
		
		if (!$success) {
			throw new CassandraSettingKeyspaceFailedException(
				'Using keyspace "'.$keyspace.'" failed after several attempts'
			);
		}
		
		if ($username !== null) {
			$request = new cassandra_AuthenticationRequest(
				array('credentials' => array($username, $password))
			);

			$this->client->login($request);
		}
	}
	
	/**
	 * Creates the socket to use.
	 * 
	 * @param string $host Hostname/IP
	 * @param integer $port Port number
	 * @param integer $sendTimeoutMs Send operations timeout
	 * @param integer $receiveTimeoutMs Receive operations timeout
	 * @return TSocket Initiated socket connection
	 */
	protected function createSocket(
		$host,
		$port,
		$sendTimeoutMs,
		$receiveTimeoutMs
	) {
		$socket = new TSocket($host, $port);
		
		if ($sendTimeoutMs !== null) {
			$socket->setSendTimeout($sendTimeoutMs);
		}
		
		if ($receiveTimeoutMs !== null) {
			$socket->setRecvTimeout($receiveTimeoutMs);
		}
		
		return $socket;
	}
	
	/**
	 * Creates framed transport.
	 * 
	 * @param TSocket $socket Socket to base the transport on
	 * @return TFramedTransport Instance of the transport
	 */
	protected function createFramedTransport(TSocket $socket) {
		require_once THRIFT_PATH.'/transport/TFramedTransport.php';
			
		return new TFramedTransport($socket, true, true);
	}
	
	/**
	 * Creates buffered transport.
	 * 
	 * @param TSocket $socket Socket to base the transport on
	 * @return TBufferedTransport Instance of the transport
	 */
	protected function createBufferedTransport(TSocket $socket) {
		require_once THRIFT_PATH.'/transport/TBufferedTransport.php';
			
		return new TBufferedTransport($socket, 512, 512);
	}
}

/**
 * A cluster is a collection of servers and connections to them.
 * 
 * Provides handling the pool of connections.
 */
class CassandraCluster {
	
	/**
	 * Currently used keyspace name.
	 * 
	 * @var string 
	 */
	protected $keyspace;
	
	/**
	 * Currently used username if using authentication.
	 * 
	 * @var string
	 */
	protected $username;
	
	/**
	 * Currently used password if using authentication.
	 * 
	 * @var string
	 */
	protected $password;
	
	/**
	 * Array of server connection information used to connect to them.
	 * 
	 * @var array 
	 */
	protected $servers = array();
	
	/**
	 * Array of open connections to servers.
	 * 
	 * The connections are reused if already opened.
	 * 
	 * @var array 
	 */
	protected $connections = array();
	
	/**
	 * Sets the list of servers to use.
	 * 
	 * You could add the servers one-by-one using
	 * {@see CassandraCluster::registerServer()}.
	 * 
	 * @param array $servers Servers that can be connected to.
	 */
	public function __construct(array $servers = array()) {
		foreach ($servers as $server) {
			$this->registerServer(
				isset($server['host']) ? $server['host'] : '127.0.0.1',
				isset($server['port']) ? $server['port'] : 9160,
				isset($server['use-framed-transport']) ? $server['use-framed-transport'] : true,
				isset($server['send-timeout-ms']) ? $server['send-timeout-ms'] : null,
				isset($server['receive-timeout-ms']) ? $server['receive-timeout-ms'] : null
			);
		}
	}
	
	/**
	 * Closes all connections on destruction.
	 */
	public function __destruct() {
		$this->closeConnections();
	}
	
	/**
	 * Registers a new server in the cluster pool.
	 * 
	 * This does not mean that it is connected to at once but it may be used in
	 * any of the requests.
	 * 
	 * @param string $host Hostname or IP of the node
	 * @param integer $port Port of the instance
	 * @param boolean $useFramedTransport Use framed or buffered transport
	 * @param integer $sendTimeoutMs Timeout of send operations in milliseconds
	 * @param integer $receiveTimeoutMs Timeout of receive operations
	 * @return CassandraCluster Self for chaining calls
	 */
	public function registerServer(
		$host = '127.0.0.1',
		$port = 9160,
		$useFramedTransport = true,
		$sendTimeoutMs = null,
		$receiveTimeoutMs = null
	) {
		$this->servers[] = array(
			'host' => $host,
			'port' => $port,
			'use-framed-transport' => $useFramedTransport,
			'send-timeout-ms' => $sendTimeoutMs,
			'receive-timeout-ms' => $receiveTimeoutMs
		);
		
		return $this;
	}
	
	/**
	 * Starts using given keyspace for all active and future connections.
	 * 
	 * @param string $keyspace Keyspace to use
	 * @param string $username Optional username
	 * @param string $password Password
	 * @return CassandraCluster Self for chaining calls
	 */
	public function useKeyspace($keyspace, $username = null, $password = null) {
		$this->keyspace = $keyspace;
		$this->username = $username;
		$this->password = $password;
		
		$this->getConnection();

		foreach ($this->connections as $connection) {
			$connection->useKeyspace(
				$keyspace,
				$username,
				$password
			);
		}
		
		return $this;
	}
	
	/**
	 * Returns the name of currently used keyspace.
	 * 
	 * @return string 
	 */
	public function getCurrentKeyspace() {
		return $this->keyspace;
	}
	
	/**
	 * Returns the list of servers connection info in the pool.
	 * 
	 * @return array 
	 */
	public function getServers() {
		return $this->servers;
	}
	
	/**
	 * Returns a connection to one of the servers.
	 * 
	 * The connections are created from the server list at random and if a
	 * server is chosen that already has an active connection, it is reused.
	 * 
	 * If a closed connection is found in the pool, it is removed and may be
	 * reconnected to later.
	 * 
	 * It will try to connect to the servers the number of servers times two
	 * times before giving up.
	 * 
	 * @return CassandraConnection Connection to one of the nodes
	 * @throws CassandraConnectionFailedException If all connections failed
	 */
	public function getConnection() {
		if (empty($this->servers)) {
			throw new CassandraConnectionFailedException(
				'Unable to create connection, the cluster server pool is empty'
			);
		}
			
		$serverCount = count($this->servers);
		$attemptsLeft = $serverCount * 2;
		
		while ($attemptsLeft-- > 0) {
			$randomServerIndex = mt_rand(0, $serverCount - 1);
			
			if (isset($this->connections[$randomServerIndex])) {
				if (!$this->connections[$randomServerIndex]->isOpen()) {
					unset($this->connections[$randomServerIndex]);
					
					continue;
				}

				return $this->connections[$randomServerIndex];
			} else {
				$server = $this->servers[$randomServerIndex];
				
				try {
					$this->connections[$randomServerIndex] = new CassandraConnection(
						$server['host'],
						$server['port'],
						$server['use-framed-transport'],
						$server['send-timeout-ms'],
						$server['receive-timeout-ms']
					);
					
					$this->connections[$randomServerIndex]->useKeyspace(
						$this->keyspace,
						$this->username,
						$this->password
					);
					
					return $this->connections[$randomServerIndex];
				} catch (TException $e) {
					continue;
				}
			}
		}
		
		throw new CassandraConnectionFailedException(
			'Connecting to any of the '.$serverCount.' nodes failed'
		);
	}
	
	/**
	 * Closes all open connections.
	 * 
	 * @return CassandraCluster Self for chaining calls
	 */
	public function closeConnections() {
		foreach ($this->connections as $connection) {
			$connection->close();
		}
		
		$this->connections = array();
		
		return $this;
	}
}

/**
 * The main Cassandra client class providing means to manage keyspaces and
 * column families, get info about schema, fetch and store data.
 */
class Cassandra {
	
	/**
	 * Array of named singleton instances.
	 * 
	 * @var array 
	 */
	protected static $instances = array();
	
	/**
	 * Array of Cassandra low-level method names that require a keyspace to
	 * be selected. Populated as needed.
	 * 
	 * @var array 
	 */
	protected static $keyspaceRequiredMethods;
	
	/**
	 * Key tokens in get() method requiring escaping.
	 * 
	 * @var array 
	 */
	protected static $requestKeyTokens = array('.', ':', ',', '-', '|');
	
	/**
	 * The Cassandra cluster to use.
	 * 
	 * @var CassandraCluster 
	 */
	protected $cluster;
	
	/**
	 * Maximum number of times to retry failed calls to Cassandra.
	 * 
	 * Use {@see Cassandra::setMaxCallRetries()} to change.
	 * 
	 * @var integer 
	 */
	protected $maxCallRetries = 5;
	
	/**
	 * Default maximum number of columns to fetch on range queries.
	 * 
	 * @var integer 
	 */
	protected $defaultColumnCount = 100;
	
	/**
	 * Array of column families.
	 * 
	 * If a single column family is requested more than once during a single
	 * request, the CassandraColumnFamily object is created only once.
	 * 
	 * @var array
	 */
	protected $columnFamilies = array();
	
	/**
	 * Authentication details per keyspace.
	 * 
	 * @var array 
	 */
	protected $keyspaceAuthentication = array();
	
	/**
	 * Should key names and values be automatically packed to correct format
	 * based on column metadata.
	 * 
	 * @var boolean 
	 */
	protected $autopack = true;
	
	/**
	 * It is enough if a single node replies.
	 * 
	 * This makes reads and writes fast, but it also means that depending on
	 * what else is reading and writing, it's possible that they could briefly
	 * give conflicting answers.
	 */
	const CONSISTENCY_ONE = cassandra_ConsistencyLevel::ONE;
	
	/**
	 * Majority of the nodes holding the data must reply.
	 * 
	 * If you have replication factor of 3 then it's enough if two of the
	 * nodes holding the data are up and reply. You need to have a replication
	 * factor of atleast three for this to work differently from all and should
	 * use odd number for replication factor.
	 */
	const CONSISTENCY_QUORUM = cassandra_ConsistencyLevel::QUORUM;
	
	/**
	 * Only meaningful for writes and means as soon as a write is received by
	 * any node, the call returns success.
	 * 
	 * This occurs when your client might be connecting to node 5 but the nodes
	 * responsible for it are 6-8. The difference between ONE and ANY
	 * is that with ANY, as soon as node 5 receives the write, it returns
	 * success (but nodes 6-8 could be down or whatever). CL::ONE means that if
	 * you write to node 5, either 6, 7, or 8 have to return success before
	 * node 5 returns success.
	 */
	const CONSISTENCY_ANY = cassandra_ConsistencyLevel::ANY;
	
	/**
	 * Returns success only if all the nodes holding the data respond.
	 * 
	 * This makes sure that all the nodes get the same data, but includes a
	 * performance penalty and also if a single node of the replication group
	 * is down, it's not possible to read or write the data as the requirement
	 * can not be fulfilled.
	 */
	const CONSISTENCY_ALL = cassandra_ConsistencyLevel::ALL;
	
	/**
	 * Standard column type.
	 */
	const COLUMN_STANDARD = 'Standard';
	
	/**
	 * Super column type.
	 */
	const COLUMN_SUPER = 'Super';
	
	/**
	 * ASCII text type.
	 */
	const TYPE_ASCII = 'AsciiType';
	
	/**
	 * Simplest binary type
	 */
	const TYPE_BYTES = 'BytesType';
	
	/**
	 * Used for a non-time based comparison. It is compared lexically, by byte
	 * value.
	 * 
	 * (UUID) are a standardized unique indentifier in the form of a 128 bit
	 * number. In it's canonical form UUIDs are represented by a 32 digit
	 * hexadecimal number in the form of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
	 */
	const TYPE_LEXICAL_UUID = 'LexicalUUIDType';
	
	/**
	 * Used for a time based comparison. It uses a version 1 UUID.
	 * 
	 * (UUID) are a standardized unique indentifier in the form of a 128 bit
	 * number. In it's canonical form UUIDs are represented by a 32 digit
	 * hexadecimal number in the form of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
	 */
	const TYPE_TIME_UUID = 'TimeUUIDType';
	
	/**
	 * Long data type.
	 */
	const TYPE_LONG = 'LongType';
	
	/**
	 * Simple integer data type.
	 */
	const TYPE_INTEGER = 'IntegerType';
	
	/**
	 * UTF8 international text data type.
	 */
	const TYPE_UTF8 = 'UTF8Type';
	
	/**
	 * Equality comparator used in where queries.
	 */
	const OP_EQ = cassandra_IndexOperator::EQ;
	
	/**
	 * Strict less-than comparator.
	 */
	const OP_LT = cassandra_IndexOperator::LT;
	
	/**
	 * Strict greater-than comparator.
	 */
	const OP_GT = cassandra_IndexOperator::GT;
	
	/**
	 * Less-than-equals comparator.
	 */
	const OP_LTE = cassandra_IndexOperator::LTE;
	
	/**
	 * Greater-than-equals comparator.
	 */
	const OP_GTE = cassandra_IndexOperator::GTE;
	
	/**
	 * Default simple placement strategy not taking network topology into
	 * account.
	 */
	const PLACEMENT_SIMPLE = 'org.apache.cassandra.locator.SimpleStrategy';
	
	/**
	 * Network topology aware placement strategy.
	 */
	const PLACEMENT_NETWORK
		= 'org.apache.cassandra.locator.NetworkTopologyStrategy';
	
	/**
	 * Old network topology aware placement strategy.
	 */
	const PLACEMENT_OLD_NETWORK
		= 'org.apache.cassandra.locator.OldNetworkTopologyStrategy';
	
	/**
	 * Keys index type, currently only one supported.
	 */
	const INDEX_KEYS = 0;
	
	/**
	 * Sets the list of servers to use and whether keys and values should be
	 * automatically packed to the correct format as defined by column families
	 * column metadata.
	 * 
	 * @param array $servers Array of server connection details.
	 * @param type $autopack Should keys and data be autopacked.
	 */
	private function __construct(array $servers = array(), $autopack = true) {
		$this->cluster = new CassandraCluster($servers);
		$this->autopack = $autopack;
	}
	
	/**
	 * Prevent users cloning the instance.
	 */
	public function __clone() {
		trigger_error('Clone is not allowed.', E_USER_ERROR);
	}
	
	/**
	 * Creates a new named cassandra instance.
	 * 
	 * The name can be used in {@see Cassandra::getInstance()} to fetch the
	 * named singleton anywhere in the project.
	 * 
	 * @param array $servers List of seed servers to connect to
	 * @param type $name Name of the instance
	 * @return Cassandra New cassandra instance
	 */
	public static function createInstance(array $servers, $name = 'main') {
		self::$instances[$name] = new self($servers);
		
		return self::$instances[$name];
	}
	
	/**
	 * Returns named singleton instance.
	 * 
	 * Name defaults to "main" the same as createInstance() so when using a
	 * single connection pool, the name needs not to be set on neither.
	 * 
	 * @param string $name Name of the instance to fetch
	 * @return Cassandra The instance
	 * @throws CassandraInvalidRequestException If no such instance exists
	 */
	public static function getInstance($name = 'main') {
		if (!isset(self::$instances[$name])) {
			throw new CassandraInvalidRequestException(
				'Instance called "'.$name.'" does not exist'
			);
		}
		
		return self::$instances[$name];
	}
	
	/**
	 * Registers a keyspace with authentication info.
	 * 
	 * @param string $keyspace Keyspace name
	 * @param string $username The username
	 * @param string $password Password
	 */
	protected function registerKeyspace(
		$keyspace,
		$username = null,
		$password = null
	) {
		$this->keyspaceAuthentication[$keyspace] = array(
			'username' => $username,
			'password' => $password
		);
	}
	
	/**
	 * Start using a new keyspace.
	 * 
	 * If the keyspace requires authentication, the username and password of it
	 * should be provided the first time this method is called. The username
	 * and password are stored so on next calls, they are used automatically if
	 * exist.
	 * 
	 * @param string $keyspace Keyspace name
	 * @param string $username The username
	 * @param string $password Password
	 * @return Cassandra Self for call chaining
	 */
	public function useKeyspace($keyspace, $username = null, $password = null) {
		if (!empty($username)) {
			$this->registerKeyspace($keyspace, $username, $password);
		} else if (isset($this->keyspaceAuthentication[$keyspace])) {
			$username = $this->keyspaceAuthentication[$keyspace]['username'];
			$password = $this->keyspaceAuthentication[$keyspace]['password'];
		}
		
		$this->cluster->useKeyspace($keyspace, $username, $password);
		
		return $this;
	}
	
	/**
	 * Returns the Cassandra cluster of servers.
	 * 
	 * @return CassandraCluster
	 */
	public function getCluster() {
		return $this->cluster;
	}
	
	/**
	 * Returns random connection to a node.
	 * 
	 * @return CassandraConnection
	 */
	public function getConnection() {
		return $this->cluster->getConnection();
	}
	
	/**
	 * Closes all open connections to nodes.
	 * 
	 * Proxies the call to cluster.
	 */
	public function closeConnections() {
		$this->cluster->closeConnections();
	}
	
	/**
	 * Return the low-level thrift client.
	 * 
	 * @return CassandraClient
	 */
	public function getClient() {
		return $this->cluster->getConnection()->getClient();
	}
	
	/**
	 * Sets the maximum number of times a call to Cassandra will be retried
	 * should it fail for any reason.
	 * 
	 * @param integer $retryCount Number of times to retry, defaults to 5.
	 * @return Cassandra Self for call chaining
	 */
	public function setMaxCallRetries($retryCount) {
		$this->maxCallRetries = $retryCount;
		
		return $this;
	}
	
	/**
	 * Sets the default number of columns to fetch at maximum.
	 * 
	 * @param integer $columnCountLimit The limit
	 */
	public function setDefaultColumnCount($columnCountLimit) {
		$this->defaultColumnCount = $columnCountLimit;
	}
	
	/**
	 * Makes a call to a Cassandra node.
	 * 
	 * This method accepts a variable number of parameters where the first one
	 * is the Cassandra client method name and the rest the parameters to pass
	 * to it.
	 * 
	 * If a call fails, it will be retried for {@see Cassandra::$maxCallRetries}
	 * times, backing off (waiting) a bit more each time to prevent flooding.
	 * 
	 * @return mixed The returned value
	 * @throws CassandraInvalidRequestException If the request is invalid
	 * @throws CassandraMaxRetriesException If The call failed all retries
	 */
	public function call(/*$methodName, $arg1, $arg2 */) {
		$args = func_get_args();
		$methodName = array_shift($args);
		
		$tries = $this->maxCallRetries;
		$lastException = null;
		
		$keyspaceRequiredMethods = self::getKeyspaceRequiredMethods();
		
		if (
			in_array($methodName, $keyspaceRequiredMethods)
			&& $this->cluster->getCurrentKeyspace() === null
		) {
			throw new CassandraInvalidRequestException(
				'Unable to call "'.$methodName.'", no keyspace has been set'
			);
		}
		
		$try = 0;
		
		while($tries-- > 0) {
			$client = $this->getClient();
			$try++;
			
			try {
                return call_user_func_array(array($client, $methodName), $args);
            } catch (Exception $e) {
				$lastException = $e;
				
				usleep(0.1 * pow(2, $try) * 1000000);
			}
		}

		throw new CassandraMaxRetriesException(
			'Failed calling "'.$methodName.'" the maximum of '.
			$this->maxCallRetries.' times',
			$lastException->getCode(),
			$lastException
		);
	}
	
	/**
	 * Returns ow-level keyspace description as returned by Cassandra.
	 * 
	 * Returns the result of "describe_keyspace" call without any processing and
	 * does not use cache. Generally you want to use the more friendly version
	 * {@see Cassandra::getKeyspaceSchema()}.
	 * 
	 * If no keyspace name is defined, the currently active keyspace is used.
	 * 
	 * @param string $keyspace Optional keyspace name.
	 * @return array Keyspace description as given by Cassandra 
	 */
	public function describeKeyspace($keyspace = null) {
		if ($keyspace === null) {
			$keyspace = $this->cluster->getCurrentKeyspace();
		}
		
		return $this->call('describe_keyspace', $keyspace);
	}
	
	/**
	 * Returns processed keyspace schema description that is also cached if
	 * possible (APC caching enabled).
	 * 
	 * @param string $keyspace Optional keyspace, current used if not set
	 * @param boolean $useCache Should caching be used if possible
	 * @return array Keyspace schema description with column families metadata 
	 */
	public function getKeyspaceSchema($keyspace = null, $useCache = true) {
		if ($keyspace === null) {
			$keyspace = $this->cluster->getCurrentKeyspace();
		}
		
		$cacheKey = 'cassandra.schema|'.$keyspace;

		$schema = false;
		$storeSchema = false;
		
		if ($useCache && function_exists('apc_fetch')) {
			$schema = apc_fetch($cacheKey);
			$storeSchema = true;
		}

		if ($schema !== false) {
			return $schema;
		}

		$info = $this->describeKeyspace($keyspace);
		
		$schema = array(
			'name' => $info->name,
			'placement-strategy' => $info->strategy_class,
			'placement-strategy-options' => $info->strategy_options,
			'replication-factor' => $info->replication_factor,
			'column-families' => array()
		);
		
		foreach ($info->cf_defs as $columnFamily) {
			$isSuper = $columnFamily->column_type == 'Super' ? true : false;
			
			$schema['column-families'][$columnFamily->name] = array(
				'name' => $columnFamily->name,
				'super' => $isSuper,
				'column-type' =>
					$isSuper
						? CassandraUtil::extractType(
							$columnFamily->subcomparator_type
						)
						: CassandraUtil::extractType(
							$columnFamily->comparator_type
						),
				'super-type' =>
					$isSuper
						? CassandraUtil::extractType(
							$columnFamily->comparator_type
						)
						: null,
				'data-type' => CassandraUtil::extractType(
					$columnFamily->default_validation_class
				),
				'column-data-types' => array()
			);

			if (
				is_array($columnFamily->column_metadata)
				&& !empty($columnFamily->column_metadata)
			) {
				foreach ($columnFamily->column_metadata as $column) {
					$schema['column-families'][$columnFamily->name]['column-data-types'][$column->name]
						= CassandraUtil::extractType($column->validation_class);
				}
			}
		}
		
		if ($storeSchema) {
			apc_store($cacheKey, $schema, 3600);
		}
		
		return $schema;
	}
	
	/**
	 * Returns the version of the Cassandra node.
	 * 
	 * @return string 
	 */
	public function getVersion() {
		return $this->call('describe_version');
	}
	
	/**
	 * Column family factory for manipulating a specific column family.
	 * 
	 * @param string $name Name of the column family.
	 * @return CassandraColumnFamily 
	 */
	public function cf($name) {
		if (!isset($this->columnFamilies[$name])) {
			$this->columnFamilies[$name] = new CassandraColumnFamily(
				$this,
				$name,
				Cassandra::CONSISTENCY_ONE,
				Cassandra::CONSISTENCY_ONE,
				$this->autopack
			);
		}
		
		return $this->columnFamilies[$name];
	}
	
	/**
	 * Column family factory for manipulating a specific column family.
	 * 
	 * Alias to {@see Cassandra::cf()} for people preferring full names.
	 * 
	 * @param string $name Name of the column family.
	 * @return CassandraColumnFamily 
	 */
	public function columnFamily($name) {
		return $this->cf($name);
	}
	
	/**
	 * Requests some data as a single request string.
	 * 
	 * Supported patterns:
	 * - family.key
	 * - family.key:col1,col2,coln
	 * - family.key:col1-col2
	 * - family.key:col1-col2|100
	 * - family\.name:key\:name:col\.1,col\|2|100
	 * 
	 * In all of the parts, the following characters shoudl be escaped (\. etc)
	 * '.', ':', ',', '-', '|'
	 * 
	 * @param string $request The request string, see patterns above
	 * @param integer $consistency Consistency level, use constants, has default
	 * @throws CassandraInvalidPatternException If equest pattern is invalid
	 */
	public function get($request, $consistency = null) {
		$details = $this->parseRequest($request);

		return $this->cf($details['column-family'])->get(
			$details['key'],
			$details['columns'],
			$details['start-column'],
			$details['end-column'],
			$details['reversed'],
			$details['column-count'],
			$details['super-column'],
			$consistency
		);
	}
	
	/**
	 * Sets a key value. The key should include column family name as first
	 * part before fullstop so key "user.john" would set key "john" in column
	 * family called "user".
	 * 
	 * Used for both inserting new data and updating existing. The columns is
	 * expected to be key-value pairs with keys as column names and values as
	 * column values.
	 * 
	 * @param string $key The key containing column family and row key
	 * @param array $columns Column names and their values
	 * @param integer $consistency Consistency to use, default used if not set
	 * @return integer Timestamp of the insterted/updated item 
	 */
	public function set($key, array $columns, $consistency = null) {
		$dotPosition = mb_strpos($key, '.');
		
		if ($dotPosition === false) {
			throw new CassandraInvalidPatternException(
				'Unable to set "'.$key.'", expected the column family name and'.
				'key name seperated by a fullstop'
			);
		}
		
		$columnFamilyName = mb_substr($key, 0, $dotPosition);
		$keyName = mb_substr($key, $dotPosition + 1);
		
		return $this->cf($columnFamilyName)->set(
			$keyName,
			$columns,
			$consistency
		);
	}
	
	/**
	 * Creates a new keyspace.
	 * 
	 * Note that the replication factor means how many nodes hold a single
	 * piece of information, not to how many nodes its replicated to so a
	 * replication factor of one means that a single node has data and no
	 * replication is performed.
	 * 
	 * @param string $name Name of the keyspace
	 * @param integer $replicationFactor How many nodes hold each piece of data
	 * @param string $placementStrategyClass Data placement strategy
	 * @param array $placementStrategyOptions Strategy options
	 * @return boolean Was creating the keyspace successful
	 * @throws Exception If anything goes wrong
	 */
	public function createKeyspace(
		$name,
		$replicationFactor = 1,
		$placementStrategyClass = self::PLACEMENT_SIMPLE,
		$placementStrategyOptions = null
	) {
		$def = new cassandra_KsDef();
		
		$def->name = $name;
		$def->strategy_class = $placementStrategyClass;
		$def->strategy_options = $placementStrategyOptions;
		$def->cf_defs = array();
		$def->replication_factor = $replicationFactor;
		
		return $this->call('system_add_keyspace', $def);
	}
	
	/**
	 * Updates keyspace information.
	 * 
	 * Note that the replication factor means how many nodes hold a single
	 * piece of information, not to how many nodes its replicated to so a
	 * replication factor of one means that a single node has data and no
	 * replication is performed.
	 * 
	 * @param string $name Name of the keyspace
	 * @param integer $replicationFactor How many nodes hold each piece of data
	 * @param string $placementStrategyClass Data placement strategy
	 * @param array $placementStrategyOptions Strategy options
	 * @return boolean Was creating the keyspace successful
	 * @throws Exception If anything goes wrong
	 */
	public function updateKeyspace(
		$name,
		$replicationFactor = 1,
		$placementStrategyClass = self::PLACEMENT_SIMPLE,
		$placementStrategyOptions = null
	) {
		$def = new cassandra_KsDef();
		
		$def->name = $name;
		$def->strategy_class = $placementStrategyClass;
		$def->strategy_options = $placementStrategyOptions;
		$def->cf_defs = array();
		$def->replication_factor = $replicationFactor;
		
		return $this->call('system_update_keyspace', $def);
	}
	
	/**
	 * Drops an entire keyspace.
	 * 
	 * @param string $name Name of the keyspace
	 * @return boolean Was dropping successful
	 * @throws Exception If anything goes wrong
	 */
	public function dropKeyspace($name) {
		return $this->call('system_drop_keyspace', $name);
	}
	
	/**
	 * Creates a new standard column family.
	 * 
	 * @param string $keyspace Keyspace to create in
	 * @param string $name Name of the column family
	 * @param array $columns Column metadata
	 * @param string $comparatorType Key comparator type
	 * @param string $defaultValidationClass Default column value type
	 * @param string $comment Free text comment
	 * @param integer $rowCacheSize Row cache size
	 * @param integer $keyCacheSize Key cache size
	 * @param float $readRepairChance Change for read repair 0..1
	 * @param integer $cgGraceSeconds Carbage collection period
	 * @param integer $minCompactionThreshold Minimum compaction threshold
	 * @param integer $maxCompactionThreshold Maximum compaction threshold
	 * @param integer $rowCacheSavePeriodSeconds Row cache saving period
	 * @param integer $keyCacheSavePeriodSeconds Key cache saving period
	 * @param integer $memtableFlushAfterMins Memtable flush after minutes
	 * @param integer $memtableFlushAfterThroughputMb Memtable flush after data
	 * @param integer $memtableFlushAfterOpsMillions Flush after operations
	 * @return boolean Was creating the column family successful
	 */
	public function createStandardColumnFamily(
		$keyspace,
		$name,
		$columns = array(),
		$comparatorType = Cassandra::TYPE_UTF8,
		$defaultValidationClass = Cassandra::TYPE_UTF8,
		$comment = null,
		$rowCacheSize = null,
		$keyCacheSize = null,
		$readRepairChance = null,
		$cgGraceSeconds = null,
		$minCompactionThreshold = null,
		$maxCompactionThreshold = null,
		$rowCacheSavePeriodSeconds = null,
		$keyCacheSavePeriodSeconds = null,
		$memtableFlushAfterMins = null,
		$memtableFlushAfterThroughputMb = null,
		$memtableFlushAfterOpsMillions = null
	) {
		return $this->createColumnFamily(
			$keyspace,
			$name,
			Cassandra::COLUMN_STANDARD,
			$columns,
			$comparatorType,
			null,
			$defaultValidationClass,
			$comment,
			$rowCacheSize,
			$keyCacheSize,
			$readRepairChance,
			$cgGraceSeconds,
			$minCompactionThreshold,
			$maxCompactionThreshold,
			$rowCacheSavePeriodSeconds,
			$keyCacheSavePeriodSeconds,
			$memtableFlushAfterMins,
			$memtableFlushAfterThroughputMb,
			$memtableFlushAfterOpsMillions
		);
	}
	
	/**
	 * Creates a new super column family.
	 * 
	 * @param string $keyspace Keyspace to create in
	 * @param string $name Name of the column family
	 * @param array $columns Column metadata
	 * @param string $comparatorType Key comparator type
	 * @param string $defaultValidationClass Default column value type
	 * @param string $comment Free text comment
	 * @param integer $rowCacheSize Row cache size
	 * @param integer $keyCacheSize Key cache size
	 * @param float $readRepairChance Change for read repair 0..1
	 * @param integer $cgGraceSeconds Carbage collection period
	 * @param integer $minCompactionThreshold Minimum compaction threshold
	 * @param integer $maxCompactionThreshold Maximum compaction threshold
	 * @param integer $rowCacheSavePeriodSeconds Row cache saving period
	 * @param integer $keyCacheSavePeriodSeconds Key cache saving period
	 * @param integer $memtableFlushAfterMins Memtable flush after minutes
	 * @param integer $memtableFlushAfterThroughputMb Memtable flush after data
	 * @param integer $memtableFlushAfterOpsMillions Flush after operations
	 * @return boolean Was creating the column family successful
	 */
	public function createSuperColumnFamily(
		$keyspace,
		$name,
		$columns = array(),
		$comparatorType = Cassandra::TYPE_UTF8,
		$subcomparatorType = Cassandra::TYPE_UTF8,
		$defaultValidationClass = Cassandra::TYPE_UTF8,
		$comment = null,
		$rowCacheSize = null,
		$keyCacheSize = null,
		$readRepairChance = null,
		$cgGraceSeconds = null,
		$minCompactionThreshold = null,
		$maxCompactionThreshold = null,
		$rowCacheSavePeriodSeconds = null,
		$keyCacheSavePeriodSeconds = null,
		$memtableFlushAfterMins = null,
		$memtableFlushAfterThroughputMb = null,
		$memtableFlushAfterOpsMillions = null
	) {
		foreach ($columns as $column) {
			if (
				array_key_exists('index-type', $column)
				|| array_key_exists('index-name', $column)
			) {
				throw new CassandraUnsupportedException(
					'Secondary indexes are not supported by supercolumns'
				);
			}
		}
		
		return $this->createColumnFamily(
			$keyspace,
			$name,
			Cassandra::COLUMN_SUPER,
			$columns,
			$comparatorType,
			$subcomparatorType,
			$defaultValidationClass,
			$comment,
			$rowCacheSize,
			$keyCacheSize,
			$readRepairChance,
			$cgGraceSeconds,
			$minCompactionThreshold,
			$maxCompactionThreshold,
			$rowCacheSavePeriodSeconds,
			$keyCacheSavePeriodSeconds,
			$memtableFlushAfterMins,
			$memtableFlushAfterThroughputMb,
			$memtableFlushAfterOpsMillions
		);
	}
	
	/**
	 * Creates a new column family, either standard or super.
	 * 
	 * You might want to use the {@see Cassandra::createStandardColumnFamily()}
	 * and {@see Cassandra::createSuperColumnFamily()} that proxy to this
	 * method.
	 * 
	 * @param string $keyspace Keyspace to create in
	 * @param string $name Name of the column family
	 * @param array $columns Column metadata
	 * @param string $comparatorType Key comparator type
	 * @param string $defaultValidationClass Default column value type
	 * @param string $comment Free text comment
	 * @param integer $rowCacheSize Row cache size
	 * @param integer $keyCacheSize Key cache size
	 * @param float $readRepairChance Change for read repair 0..1
	 * @param integer $cgGraceSeconds Carbage collection period
	 * @param integer $minCompactionThreshold Minimum compaction threshold
	 * @param integer $maxCompactionThreshold Maximum compaction threshold
	 * @param integer $rowCacheSavePeriodSeconds Row cache saving period
	 * @param integer $keyCacheSavePeriodSeconds Key cache saving period
	 * @param integer $memtableFlushAfterMins Memtable flush after minutes
	 * @param integer $memtableFlushAfterThroughputMb Memtable flush after data
	 * @param integer $memtableFlushAfterOpsMillions Flush after operations
	 * @return boolean Was creating the column family successful
	 */
	public function createColumnFamily(
		$keyspace,
		$name,
		$columnType = Cassandra::COLUMN_STANDARD,
		$columns = array(),
		$comparatorType = Cassandra::TYPE_UTF8,
		$subcomparatorType = null,
		$defaultValidationClass = Cassandra::TYPE_UTF8,
		$comment = null,
		$rowCacheSize = null,
		$keyCacheSize = null,
		$readRepairChance = null,
		$cgGraceSeconds = null,
		$minCompactionThreshold = null,
		$maxCompactionThreshold = null,
		$rowCacheSavePeriodSeconds = null,
		$keyCacheSavePeriodSeconds = null,
		$memtableFlushAfterMins = null,
		$memtableFlushAfterThroughputMb = null,
		$memtableFlushAfterOpsMillions = null
	) {
		$columnMetadata = null;
		
		if (!empty($columns)) {
			$columnMetadata = array();
			
			foreach ($columns as $column) {
				$columnMetadata[] = $this->createColumnDefinition($column);
			}
		}
		
		$def = new cassandra_CfDef();
		
		$def->keyspace = $keyspace;
		$def->name = $name;
		$def->column_type = $columnType;
		$def->comparator_type = $comparatorType;
		$def->subcomparator_type = $subcomparatorType;
		$def->comment = $comment;
		$def->row_cache_size = $rowCacheSize;
		
		if ($keyCacheSize !== null) {
			$def->key_cache_size = $keyCacheSize;
		}
		
		$def->read_repair_chance = $readRepairChance;
		$def->column_metadata = $columnMetadata;
		$def->gc_grace_seconds = $cgGraceSeconds;
		$def->default_validation_class = $defaultValidationClass;
		$def->min_compaction_threshold = $minCompactionThreshold;
		$def->max_compaction_threshold = $maxCompactionThreshold;
		$def->row_cache_save_period_in_seconds = $rowCacheSavePeriodSeconds;
		$def->key_cache_save_period_in_seconds = $keyCacheSavePeriodSeconds;
		$def->memtable_flush_after_mins = $memtableFlushAfterMins;
		$def->memtable_throughput_in_mb = $memtableFlushAfterThroughputMb;
		$def->memtable_operations_in_millions = $memtableFlushAfterOpsMillions;

		return $this->call('system_add_column_family', $def);
	}
	
	/**
	 * Truncates a column family of all of its data (entries).
	 * 
	 * @param string $columnFamilyName Name of the column family
	 * @return boolean Was truncating successful 
	 */
	public function truncate($columnFamilyName) {
		return $this->call('truncate', $columnFamilyName);
	}
	
	/**
	 * Returns the list of low-level cassandra methods that require keyspace to
	 * be selected.
	 * 
	 * @return array
	 */
	protected static function getKeyspaceRequiredMethods() {
		if (self::$keyspaceRequiredMethods === null) {
			self::$keyspaceRequiredMethods = array(
				'login',
				'get',
				'get_slice',
				'get_count',
				'multiget_slice',
				'multiget_count',
				'get_indexed_slices',
				'insert',
				'remove',
				'batch_mutate',
				'truncate',
				'describe_splits'
			);
		}
		
		return self::$keyspaceRequiredMethods;
	}

	/**
	 * Parses a get request.
	 * 
	 * @param string $request Request to parse
	 * @return array Request components 
	 */
	protected function parseRequest($request) {
		foreach(self::$requestKeyTokens as $tokenKey => $keyToken) {
			$request = str_replace(
				'\\'.$keyToken,
				'[/'.$tokenKey.'\\]',
				$request
			);
		}
		
		$components = array();
		
		$matchSuccessful = preg_match(
			'/^(.+)\.(.+)((\.(.*))|\[\])?(:(.*))?(\|(\d*)(R)?)?$/U',
			// 1     2   34  5           6 7     8  9    10        
			$request,
			$components
		);

		if (!$matchSuccessful || empty($components)) {
			throw new CassandraInvalidPatternException(
				'Invalid get request "'.$request.'" provided'
			);
		}

		$result = array(
			'column-family' => $components[1],
			'key' => $components[2],
			'columns' => null,
			'start-column' => null,
			'end-column' => null,
			'reversed' => false,
			'column-count' => $this->defaultColumnCount,
			'super-column' => null
		);
		
		$componentCount = count($components);
		
		// @codeCoverageIgnoreStart
		if ($componentCount < 3 || $componentCount > 11) {
			throw new CassandraInvalidPatternException(
				'Invalid pattern, expected between 3 and 11 components, got '.
				$componentCount.', this should not happen'
			);
		}
		// @codeCoverageIgnoreEnd
		
		if ($componentCount >= 6) {
			if (
				!empty($components[5])
				&& $components[5] != '[]'
			) {
				$result['super-column'] = $components[5];
			}
			
			if ($componentCount >= 8) {
				if (!empty($components[7])) {
					if (strpos($components[7], ',') !== false) {
						$columns = explode(',', $components[7]);

						foreach ($columns as $columnIndex => $columnName) {
							$columns[$columnIndex] = trim($columnName);
						}

						$result['columns'] = $columns;
					} else if (strpos($components[7], '-') !== false) {
						$rangeColumns = explode('-', $components[7]);

						if (count($rangeColumns) > 2) {
							throw new CassandraInvalidPatternException(
								'Expected no more than 2 columns '.
								'to define a range'
							);
						}

						$result['start-column'] = trim($rangeColumns[0]);
						$result['end-column'] = trim($rangeColumns[1]);
					} else {
						$result['columns'] = array(trim($components[7]));
					}
				}

				if ($componentCount >= 10 && !empty($components[9])) {
					$columnCount = (int)$components[9];

					$result['column-count'] = $columnCount;
				}

				if (
					$componentCount == 11
					&& strtoupper($components[10]) == 'R'
				) {
					$result['reversed'] = true;
				}
			}
		}
		
		foreach ($result as $key => $item) {
			$result[$key] = self::unescape($item);
		}
	
		return $result;
	}
	
	/**
	 * Escapes keys used in get requests.
	 * 
	 * @param string $value Value to escape
	 * @return string escaped value
	 */
	public static function escape($value) {
		foreach(self::$requestKeyTokens as $keyToken) {
			$value = str_replace(
				$keyToken,
				'\\'.$keyToken,
				$value
			);
		}
		
		return $value;
	}
	
	/**
	 * Unescapes keys used in get requests.
	 * 
	 * @param string $value Value to unescape
	 * @return string unescaped value
	 */
	public static function unescape($value) {
		if (empty($value)) {
			return $value;
		}
		else if (is_array($value)) {
			foreach ($value as $key => $item) {
				$value[$key] = self::unescape($item);
			}
		} else if (is_string($value)) {
			foreach(self::$requestKeyTokens as $tokenKey => $keyToken) {
				$value = str_replace(
					'[/'.$tokenKey.'\\]',
					$keyToken,
					$value
				);
			}
		}
		
		return $value;
	}
	
	/**
	 * Creates column definition.
	 * 
	 * @param array $info Column info
	 * @return cassandra_ColumnDef Column definition
	 */
	protected function createColumnDefinition(array $info) {
		$def = new cassandra_ColumnDef();

		$def->name = $info['name'];
		
		if (!empty($info['type'])) {
			$def->validation_class = $info['type'];
		}
		
		if (isset($info['index-type'])) {
			$def->index_type = $info['index-type'];
		}
		
		if (!empty($info['index-name'])) {
			$def->index_type = $info['index-name'];
		}
		
		return $def;
	}
}

class CassandraColumnFamily {
	
	protected $cassandra;
	protected $name;
	protected $defaultReadConsistency;
	protected $defaultWriteConsistency;
	protected $autopack;
	protected $schema;
	
	public function __construct(
		Cassandra $cassandra,
		$name,
		$defaultReadConsistency = Cassandra::CONSISTENCY_ONE,
		$defaultWriteConsistency = Cassandra::CONSISTENCY_ONE,
		$autopack = true
	) {
		$this->cassandra = $cassandra;
		$this->name = $name;
		$this->defaultReadConsistency = $defaultReadConsistency;
		$this->defaultWriteConsistency = $defaultWriteConsistency;
		$this->autopack = $autopack;
		$this->schema = null;
	}
	
	public function getCassandra() {
		return $this->cassandra;
	}
	
	public function getSchema($useCache = true) {
		if ($this->schema === null) {
			$keyspaceSchema = $this->cassandra->getKeyspaceSchema(
				null,
				$useCache
			);

			if (!isset($keyspaceSchema['column-families'][$this->name])) {
				throw new CassandraColumnFamilyNotFoundException(
					'Schema for column family "'.$this->name.'" not found'
				);
			}
			
			$this->schema = $keyspaceSchema['column-families'][$this->name];
		}
		
		return $this->schema;
	}
	
	public function getColumnNameType($useCache = true) {
		$schema = $this->getSchema($useCache);
		
		if ($schema['super']) {
			return $schema['super-type'];
		} else {
			return $schema['column-type'];
		}
	}
	
	public function getColumnValueType($columnName, $useCache = true) {
		$schema = $this->getSchema($useCache);
			
		if (isset($schema['column-data-types'][$columnName])) {
			return $schema['column-data-types'][$columnName];
		}
		
		return 'BytesType';
	}
	
	public function getAll($key, $superColumn = null, $consistency = null) {
		return $this->get(
			$key,
			null,
			null,
			null,
			false,
			100,
			$superColumn,
			$consistency
		);
	}
	
	public function getColumns(
		$key,
		array $columns,
		$superColumn = null,
		$consistency = null
	) {
		return $this->get(
			$key,
			$columns,
			null,
			null,
			false,
			100,
			$superColumn,
			$consistency
		);
	}
	
	public function getColumnRange(
		$key,
		$startColumn,
		$endColumn,
		$superColumn = null,
		$consistency = null
	) {
		return $this->get(
			$key,
			null,
			$startColumn,
			$endColumn,
			false,
			100,
			$superColumn,
			$consistency
		);
	}
	
	public function get(
		$key,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$columnsReversed = false,
		$columnCount = 100,
		$superColumn = null,
		$consistency = null
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			$columnsReversed,
			$columnCount
		);
		
		$result = $this->cassandra->call(
			'get_slice',
			$key,
			$columnParent,
			$slicePredicate,
			$consistency
		);
				
		if (count($result) == 0) {
			return null;
		}
		
		return $this->parseSliceResponse($result);
	}
	
	public function getWhere(
		array $where,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$columnsReversed = false,
		$rowCountLimit = null,
		$columnCount = 100,
		$superColumn = null,
		$consistency = null,
		$bufferSize = 1000
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			$columnsReversed,
			$columnCount
		);
		
		$indexClause = $this->createIndexClause(
			$where,
			$startColumn,
			$columnCount
		);
		
		return new CassandraIndexedDataIterator(
			$this,
			$columnParent,
			$indexClause,
			$slicePredicate,
			$consistency,
			$rowCountLimit,
			$bufferSize
		);
	}
	
	public function getMultiple(
		array $keys,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$columnsReversed = false,
		$columnCount = 100,
		$superColumn = null,
		$consistency = null,
		$bufferSize = 512
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			$columnsReversed,
			$columnCount
		);
		
		$results = array();
		$responses = array();
		
		foreach ($keys as $key) {
			$results[$key] = null;
		}
		
		$keyCount = count($keys);
		$setCount = ceil($keyCount / $bufferSize);
		
		for ($i = 0; $i < $setCount; $i++) {
			$setKeys = array_slice($keys, $i * $bufferSize, $bufferSize);
			
			$setResponse = $this->cassandra->call(
				'multiget_slice',
				$setKeys,
				$columnParent,
				$slicePredicate,
				$consistency
			);

			$responses = array_merge($responses, $setResponse);
		}

		foreach ($responses as $key => $response) {
			$results[$key] = $this->parseSliceResponse($response);
		}

		return $results;
	}
	
	public function getRange(
		$startKey = null,
		$endKey = null,
		$rowCountLimit = null,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$columnsReversed = false,
		$columnCount = 100,
		$superColumn = null,
		$consistency = null,
		$bufferSize = 512
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		if ($startKey === null) {
			$startKey = '';
		}
		
		if ($endKey === null) {
			$endKey = '';
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			$columnsReversed,
			$columnCount
		);
		
		return new CassandraRangeDataIterator(
			$this,
			$columnParent,
			$slicePredicate,
			$startKey,
			$endKey,
			$consistency,
			$rowCountLimit,
			$bufferSize
		);
	}
	
	public function getColumnCount(
		$key,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$superColumn = null,
		$consistency = null	
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			false,
			2147483647
		);

		return $this->cassandra->call(
			'get_count',
			$key,
			$columnParent,
			$slicePredicate,
			$consistency
		);
	}
	
	public function getColumnCounts(
		array $keys,
		$columns = null,
		$startColumn = null,
		$endColumn = null,
		$superColumn = null,
		$consistency = null	
	) {
		if ($columns !== null && $startColumn !== null) {
			throw new CassandraInvalidRequestException(
				'You can define either a list of columns or the start and end '.
				'columns for a range but not both at the same time'
			);
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultReadConsistency;
		}
		
		$columnParent = $this->createColumnParent($superColumn);

		$slicePredicate = $this->createSlicePredicate(
			$columns,
			$startColumn,
			$endColumn,
			false,
			2147483647
		);

		return $this->cassandra->call(
			'multiget_count',
			$keys,
			$columnParent,
			$slicePredicate,
			$consistency
		);
	}
	
	public function set(
		$key,
		array $columns,
		$consistency = null,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		if ($consistency === null) {
			$consistency = $this->defaultWriteConsistency;
		}
		
		$mutationMap = array(
			$key => array(
				$this->name => $this->createColumnMutations(
					$columns,
					$timestamp,
					$timeToLiveSeconds
				)
			)
		);
		
		$this->cassandra->call('batch_mutate', $mutationMap, $consistency);
	}
	
	public function createColumnParent($superColumnName = null) {
		$schema = $this->getSchema();
		
		$columnParent = new cassandra_ColumnParent();
		$columnParent->column_family = $this->name;
		
		if ($superColumnName !== null) {
			if ($this->autopack) {
				$columnParent->super_column = CassandraUtil::pack(
					$superColumnName,
					$schema['super-type']
				);
			} else {
				$columnParent->super_column = $superColumnName;
			}
		}
		
		return $columnParent;
	}
	
	public function createSlicePredicate(
		$columns,
		$startColumn,
		$endColumn,
		$columnsReversed,
		$columnCount
	) {
		$predicate = new cassandra_SlicePredicate();
		$schema = $this->getSchema();
		
		if (is_array($columns)) {
			if ($this->autopack) {
				$packedColumns = array();

				foreach ($columns as $columnName) {
					$columnType = $this->getColumnNameType();
					
					$packedColumns[] = CassandraUtil::pack(
						$columnName,
						$columnType
					);
				}
				
				$predicate->column_names = $packedColumns;
			} else {
				$predicate->column_names = $columns;
			}
		} else {
			if ($this->autopack) {
				if ($startColumn === null) {
					$startColumn = '';
				}
				
				if ($endColumn === null) {
					$endColumn = '';
				}
				
				if (!empty($startColumn)) {
					$columnType = $this->getColumnNameType();
					
					$startColumn = CassandraUtil::pack(
						$startColumn,
						$columnType
					);
				}
				
				if (!empty($endColumn)) {
					$columnType = $this->getColumnNameType();
					
					$endColumn = CassandraUtil::pack(
						$endColumn,
						$columnType
					);
				}
			}
			
			$sliceRange = new cassandra_SliceRange();
			$sliceRange->count = $columnCount;
			$sliceRange->reversed = $columnsReversed;
			$sliceRange->start = $startColumn;
			$sliceRange->finish = $endColumn;
			
			$predicate->slice_range = $sliceRange;
		}
		
		return $predicate;
	}
	
	public function createIndexClause(
		array $where,
		$startKey = null,
		$columnCount = 100
	) {
		$indexClause = new cassandra_IndexClause();
		$expressions = array();
		
		foreach ($where as $columnName => $value) {
			$indexExpression = new cassandra_IndexExpression();
			
			if (is_array($value)) {
				$supportedOperators = array(
					Cassandra::OP_EQ,
					Cassandra::OP_LT,
					Cassandra::OP_GT,
					Cassandra::OP_LTE,
					Cassandra::OP_GTE
				);
				
				if (
					count($value) != 3
					|| !in_array($value[1], $supportedOperators)
				) {
					throw new CassandraInvalidRequestException(
						'Invalid where clause: '.serialize($value)
					);
				}
				
				$indexExpression->column_name = CassandraUtil::pack(
					$value[0],
					$this->getColumnNameType()
				);
				
				$indexExpression->op = $value[1];
				
				$indexExpression->value = CassandraUtil::pack(
					$value[2],
					$this->getColumnValueType($value[0])
				);
			} else {
				$indexExpression->column_name = CassandraUtil::pack(
					$columnName,
					$this->getColumnNameType()
				);
				
				$indexExpression->op = Cassandra::OP_EQ;
				
				$indexExpression->value = CassandraUtil::pack(
					$value,
					$this->getColumnValueType($columnName)
				);
			}
			
			$expressions[] = $indexExpression;
		}
		
		$indexClause->expressions = $expressions;
        $indexClause->start_key = $startKey !== null ? $startKey : '';
        $indexClause->count = $columnCount;
		
		return $indexClause;
	}
	
	public function createColumnMutations(
		array $columns,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		$columnsOrSuperColumns = $this->createColumnsOrSuperColumns(
			$columns,
			$timestamp,
			$timeToLiveSeconds
		);
		
		$mutations = array();
		
		foreach ($columnsOrSuperColumns as $columnOrSuperColumn) {
			$mutation = new cassandra_Mutation();
			$mutation->column_or_supercolumn = $columnOrSuperColumn;
			
			$mutations[] = $mutation;
		}
		
		return $mutations;
	}
	
	public function createColumnsOrSuperColumns(
		array $columns,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		$results = array();
		
		foreach ($columns as $columnName => $columnValue) {
			$column = new cassandra_ColumnOrSuperColumn();
			
			if (is_array($columnValue)) {
				$column->super_column = new cassandra_SuperColumn();
				$column->super_column->name = CassandraUtil::pack(
					$columnName,
					$this->getColumnNameType()
				);
				$column->super_column->columns = $this->createColumns(
					$columnValue,
					$timestamp,
					$timeToLiveSeconds
				);
				$column->super_column->timestamp = $timestamp;
			} else {
				$column->column = new cassandra_Column();
				$column->column->name = CassandraUtil::pack(
					$columnName,
					$this->getColumnNameType()
				);
				$column->column->value = CassandraUtil::pack(
					$columnValue,
					$this->getColumnValueType($columnName)
				);
				$column->column->timestamp = $timestamp;
				$column->column->ttl = $timeToLiveSeconds;
			}
			
			$results[] = $column;
		}
		
		return $results;
	}
	
	public function createColumns(
		array $columns,
		$timestamp = null,
		$timeToLiveSeconds = null
	) {
		if ($timestamp === null) {
			$timestamp = CassandraUtil::getTimestamp();
		}
		
		$results = array();
		
		foreach ($columns as $name => $value) {
			$column = new cassandra_Column();
			$column->name = CassandraUtil::pack(
				$name,
				$this->getColumnNameType()
			);
			$column->value = CassandraUtil::pack(
				$value,
				$this->getColumnValueType($name)
			);
			$column->timestamp = $timestamp;
			$column->ttl = $timeToLiveSeconds;
			
			$results[] = $column;
		}
		
		return $results;
	}
	
	public function parseSlicesResponse(array $response) {
		$results = array();

		foreach ($response as $row) {
			$key = $row->key;
			$results[$key] = array();
			
			foreach ($row->columns as $row) {
				$results[$key] = array_merge(
					$results[$key],
					$this->parseSliceRow($row)
				);
			}
		}
		
		return $results;
	}

	public function parseSliceResponse(array $response) {
		$results = array();
		
		foreach ($response as $row) {
			$results = array_merge(
				$results,
				$this->parseSliceRow($row)
			);
		}
		
		return $results;
	}
	
	protected function parseSliceRow(cassandra_ColumnOrSuperColumn $row) {
		$result = array();
		
		if ($row->column !== null) {
			$nameType = $this->getColumnNameType();
			$valueType = $this->getColumnValueType($row->column->name);

			$name = CassandraUtil::unpack($row->column->name, $nameType);
			$value = CassandraUtil::unpack($row->column->value, $valueType);

			$result[$name] = $value;
		} else if($row->super_column !== null) {
			$superNameType = null;

			$superName = CassandraUtil::unpack(
				$row->super_column->name,
				$superNameType
			);

			$values = array();

			foreach ($row->super_column->columns as $column) {
				$nameType = $this->getColumnNameType();
				$valueType = $this->getColumnValueType($column->name);

				$name = CassandraUtil::unpack($column->name, $nameType);
				$value = CassandraUtil::unpack($column->value, $valueType);

				$values[$name] = $value;
			}

			$result[$superName] = $values;
		} else {
			// @codeCoverageIgnoreStart
			throw new Exception('Expected either normal or super column');
			// @codeCoverageIgnoreEnd
		}

		return $result;
	}
}

class CassandraUtil {
	
	public static function extractType($definition) {
		if ($definition === null or $definition == '') {
			return Cassandra::TYPE_BYTES;
		}

		$index = strrpos($definition, '.');

		if ($index === false) {
			return Cassandra::TYPE_BYTES;
		}

		return substr($definition, $index + 1);
	}
	
	public static function pack($value, $type) {
		switch ($type) {
			case Cassandra::TYPE_LONG:
				return self::packLong($value);
			
			case Cassandra::TYPE_INTEGER:
				return self::packInteger($value);
			
			case Cassandra::TYPE_ASCII:
				return self::packString($value, strlen($value));
			
			case Cassandra::TYPE_UTF8:
				if (mb_detect_encoding($value, 'UTF-8') != 'UTF-8') {
					$value = utf8_encode($value);
				}
				
				return self::packString($value, strlen($value));
				
			case Cassandra::TYPE_TIME_UUID:
				return self::packString($value, 16);
			
			case Cassandra::TYPE_LEXICAL_UUID:
				return self::packString($value, 16);
				
			default:
				return $value;
		}
	}
	
	public static function packLong($value) {
        // If we are on a 32bit architecture we have to explicitly deal with
		// 64-bit twos-complement arithmetic since PHP wants to treat all ints
		// as signed and any int over 2^31 - 1 as a float
		if (PHP_INT_SIZE == 4) {
			$neg = $value < 0;

			if ($neg) {
				$value *= - 1;
			}

			$hi = (int) ($value / 4294967296);
			$lo = (int) $value;

			if ($neg) {
				$hi = ~$hi;
				$lo = ~$lo;
				
				if (($lo & (int)0xffffffff) == (int)0xffffffff) {
					$lo = 0;
					$hi++;
				} else {
					$lo++;
				}
			}
			
			$data = pack('N2', $hi, $lo);
		} else {
			$hi = $value >> 32;
			$lo = $value & 0xFFFFFFFF;
			$data = pack('N2', $hi, $lo);
		}
		
		return $data;
	}
	
	public static function packInteger($value) {
		return pack('N', $value);
	}
	
	public static function packString($string, $length) {       
        $result = '';
		
        for($i = 0; $i < $length; $i++) {
            $result .= pack('c', ord(substr($string, $i, 1)));
		}
		
        return $result;
    }
	
	public static function unpack($value, $type) {
		switch ($type) {
			case Cassandra::TYPE_LONG:
				return self::unpackLong($value);
			
			case Cassandra::TYPE_INTEGER:
				return self::unpackInteger($value);
			
			case Cassandra::TYPE_ASCII:
				return self::unpackString($value, strlen($value));
			
			case Cassandra::TYPE_UTF8:
				return self::unpackString($value, strlen($value));
				
			case Cassandra::TYPE_TIME_UUID:
				return $value;
			
			case Cassandra::TYPE_LEXICAL_UUID:
				return $value;
				
			default:
				return $value;
		}
	}
	
	public static function unpackLong($data) {
		$arr = unpack('N2', $data);

		// If we are on a 32bit architecture we have to explicitly deal with
		// 64-bit twos-complement arithmetic since PHP wants to treat all ints
		// as signed and any int over 2^31 - 1 as a float
		if (PHP_INT_SIZE == 4) {

			$hi = $arr[1];
			$lo = $arr[2];
			$isNeg = $hi < 0;

			// Check for a negative
			if ($isNeg) {
				$hi = ~$hi & (int)0xffffffff;
				$lo = ~$lo & (int)0xffffffff;

				if ($lo == (int)0xffffffff) {
					$hi++;
					$lo = 0;
				} else {
					$lo++;
				}
			}

			// Force 32bit words in excess of 2G to pe positive - we deal wigh sign
			// explicitly below

			if ($hi & (int)0x80000000) {
				$hi &= (int)0x7fffffff;
				$hi += 0x80000000;
			}

			if ($lo & (int)0x80000000) {
				$lo &= (int)0x7fffffff;
				$lo += 0x80000000;
			}

			$value = $hi * 4294967296 + $lo;

			if ($isNeg) {
				$value = 0 - $value;
			}
		} else {
			// Upcast negatives in LSB bit
			if ($arr[2] & 0x80000000)
				$arr[2] = $arr[2] & 0xffffffff;

			// Check for a negative
			if ($arr[1] & 0x80000000) {
				$arr[1] = $arr[1] & 0xffffffff;
				$arr[1] = $arr[1] ^ 0xffffffff;
				$arr[2] = $arr[2] ^ 0xffffffff;
				
				$value = 0 - $arr[1] * 4294967296 - $arr[2] - 1;
			} else {
				$value = $arr[1] * 4294967296 + $arr[2];
			}
		}
		
		return $value;
	}
	
	public static function unpackInteger($value) {
		$unpacked = unpack('N', $value);
		
		return array_pop($unpacked);
	}
	
	public static function unpackString($value, $length) {
		$unpacked = unpack('c'.$length.'chars', $value);
        $out = '';
		
        foreach($unpacked as $element) {
            if($element > 0) {
				$out .= chr($element);
			}
		}
		
        return $out;
	}
	
	public function getTimestamp() {
		// By Zach Buller (zachbuller@gmail.com)
        $time1 = microtime();
        settype($time1, 'string'); //convert to string to keep trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs) / 100;
		
        return $time3;
	}
}

abstract class CassandraDataIterator implements Iterator {
	
	protected $columnFamily;
	protected $columnParent;
	protected $slicePredicate;
	protected $consistency;
	protected $rowCountLimit;
	protected $bufferSize;
	
	protected $buffer;
	protected $originalStartKey;
	protected $nextStartKey;
	protected $isValid;
	protected $rowsSeen;
	protected $expectedPageSize;
	protected $currentPageSize;
	
	public function __construct(
		CassandraColumnFamily $columnFamily,
		cassandra_ColumnParent $columnParent,
		cassandra_SlicePredicate $slicePredicate,
		$startKey,
		$consistency,
		$rowCountLimit,
		$bufferSize
	) {
		$this->columnFamily = $columnFamily;
		$this->columnParent = $columnParent;
		$this->slicePredicate = $slicePredicate;
		$this->consistency = $consistency;
		$this->rowCountLimit = $rowCountLimit;
		$this->bufferSize = $bufferSize;
		$this->originalStartKey = $startKey;
		$this->nextStartKey = $startKey;
		$this->buffer = null;
		
		if ($rowCountLimit !== null) {
			$this->bufferSize = min($rowCountLimit, $bufferSize);
		}
	}
	
	public function current() {
		return current($this->buffer);
	}
	
	public function key() {
		return key($this->buffer);
	}
	
	public function next() {
		$beyondLastRow = false;
		
		if (!empty($this->buffer)) {
			$this->nextStartKey = key($this->buffer);
			
			next($this->buffer);

			if (count(current($this->buffer)) == 0) {
				$this->next();
			} else {
				$key = key($this->buffer);
				
				if (isset($key)) {
					$this->rowsSeen++;
					
					if (
						$this->rowCountLimit !== null
						&& $this->rowsSeen > $this->rowCountLimit
					) {
						$this->isValid = false;
						
						return;
					}
				} else {
					$beyondLastRow = true;
				}
			}
		} else {
			$beyondLastRow = true;
		}

		if ($beyondLastRow) {
			if ($this->currentPageSize < $this->expectedPageSize) {
				$this->isValid = false;
			} else {
				$this->updateBuffer();
				
				if (count($this->buffer) == 1) {
					$this->isValid = false;
				} else {
					$this->next();
				}
			}
		}
	}
	
	public function rewind() {
		$this->rowsSeen = 0;
		$this->isValid = true;
		$this->nextStartKey = $this->originalStartKey;
		
		$this->updateBuffer();
		
		if (count($this->buffer) == 0) {
			$this->isValid = false;
			
			return;
		}
		
		if (count(current($this->buffer)) == 0) {
			$this->next();
		} else {
			$this->rowsSeen++;
		}
	}
	
	public function valid() {
		return $this->isValid;
	}
	
	public function getAll() {
		$results = array();
		
		$this->rewind();
		
		while ($this->valid()) {
			$key = $this->key();
			$value = $this->current();
			
			$results[$key] = $value;
			
			$this->next();
		}
		
		return $results;
	}
	
	abstract protected function updateBuffer();
}

class CassandraIndexedDataIterator extends CassandraDataIterator {
	
	protected $indexClause;

	public function __construct(
		CassandraColumnFamily $columnFamily,
		cassandra_ColumnParent $columnParent,
		cassandra_IndexClause $indexClause,
		cassandra_SlicePredicate $slicePredicate,
		$consistency,
		$rowCountLimit,
		$bufferSize
	) {
		parent::__construct(
			$columnFamily,
			$columnParent,
			$slicePredicate,
			$indexClause->start_key,
			$consistency,
			$rowCountLimit, 
			$bufferSize
		);
		
		$this->indexClause = $indexClause;
	}
	
	protected function updateBuffer() {
		if ($this->rowCountLimit !== null) {
			$this->indexClause->count = min(
				$this->rowCountLimit - $this->rowsSeen + 1,
				$this->bufferSize
			);
		} else {
			$this->indexClause->count = $this->bufferSize;
		}
		
		$this->expectedPageSize = $this->indexClause->count;
		$this->indexClause->start_key = $this->nextStartKey;
		
		$result = $this->columnFamily->getCassandra()->call(
			'get_indexed_slices',
			$this->columnParent,
			$this->indexClause,
			$this->slicePredicate,
			$this->consistency
		);
		
		if (count($result) == 0) {
			$this->buffer = array();
		} else {
			$this->buffer = $this->columnFamily->parseSlicesResponse($result);
		}
		
		$this->currentPageSize = count($this->buffer);
	}
}

class CassandraRangeDataIterator extends CassandraDataIterator {
	
	protected $startKey;
	protected $endKey;

	public function __construct(
		CassandraColumnFamily $columnFamily,
		cassandra_ColumnParent $columnParent,
		cassandra_SlicePredicate $slicePredicate,
		$startKey,
		$endKey,
		$consistency,
		$rowCountLimit,
		$bufferSize
	) {
		parent::__construct(
			$columnFamily,
			$columnParent,
			$slicePredicate,
			$startKey,
			$consistency,
			$rowCountLimit, 
			$bufferSize
		);
		
		$this->startKey = $startKey;
		$this->endKey = $endKey;
	}
	
	protected function updateBuffer() {
		$bufferSize = $this->bufferSize;
		
		if ($this->rowCountLimit !== null) {
			$bufferSize = min(
				$this->rowCountLimit - $this->rowsSeen + 1,
				$this->bufferSize
			);
		}
		
		$this->expectedPageSize = $bufferSize;
		
		$keyRange = new cassandra_KeyRange();
		$keyRange->start_key = $this->nextStartKey;
		$keyRange->end_key = $this->endKey;
		$keyRange->count = $bufferSize;
		
		$result = $this->columnFamily->getCassandra()->call(
			'get_range_slices',
			$this->columnParent,
			$this->slicePredicate,
			$keyRange,
			$this->consistency
		);
		
		$this->buffer = $this->columnFamily->parseSlicesResponse($result);
		$this->currentPageSize = count($this->buffer);
	}
}

class CassandraMaxRetriesException				extends Exception {};
class CassandraConnectionClosedException		extends Exception {};
class CassandraConnectionFailedException		extends Exception {};
class CassandraSettingKeyspaceFailedException	extends Exception {};
class CassandraColumnFamilyNotFoundException	extends Exception {};
class CassandraInvalidRequestException			extends Exception {};
class CassandraInvalidPatternException			extends Exception {};
class CassandraUnsupportedException				extends Exception {};