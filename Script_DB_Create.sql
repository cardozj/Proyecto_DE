create table criptos_price(
	id varchar(40) NOT NULL,
	symbol varchar (20) UNIQUE,
	name varchar (100),
	supply decimal (20,2),
	maxSupply decimal (20,2),
 	marketCapUsd decimal (20,2),
	volumeUsd24Hr decimal (20,2),
	priceUsd decimal (20,2),
	changePercent24Hr decimal (20,2),
	vwap24Hr decimal (20,2),
	explorer varchar (120),
	timestamp timestamp,
	PRIMARY key (id)
);


