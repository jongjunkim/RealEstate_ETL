var express = require('express');
var router = express.Router();

const mysql = require('mysql');
const config = require('../conf/db.js');
var conn = mysql.createConnection(config.local)

router.get('/amount/:from_date/:to_date', function(req,res) {

	var from_date = req.params.from_date;
	var to_date = req.params.to_date;

	console.log(from_date, to_date);

 	var query = 'select DealYMD, 거래금액 from real_estate_transaction where DealYMD between ? and ? and 아파트 = "두산" order by DealYMD';

	conn.query(query, [from_date, to_date], function(err, rows) {
		if(err){
			console.error(err);
			throw err;
		}

		res.statusCode = 200;
		console.log(rows);

		res.json(rows);
	});
});

router.get('/', function(req, res, next) {
    res.render('realestate', {title: 'realestate'})
});

module.exports = router;