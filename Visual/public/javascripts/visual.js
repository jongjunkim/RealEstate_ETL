$(document).ready(function() {

	$(function() {
		$("#datepicker1, #datepicker2").datepicker({
			dateFormat : 'yy-mm-dd'
		});
	});

});

function drawingGraph(data, opt) {
	Plotly.newPlot('myDiv', {
		data: data,
		layout: opt,
	});
}

function loadDailyTable() {

	var from_date = document.getElementById("datepicker1").value;
	var to_date = document.getElementById("datepicker2").value;

	if (to_date.length == 0 || from_date.length == 0) {
		from_date = "2023-01-01";
		to_date = "2023-01-02";
	}

	var url = "/realestate/amount/" + from_date + "/" + to_date;


	$.ajax({
		type: "GET",
		url: url,
		success:function(rows){
			parseDailyData(rows);
			return false;
		},
		error:function(xhr, status, err){
			console.log(xhr.responseText);
			return false;
		}
	});
}

function parseDailyData(rows) {
    var _date_list = [];
    var _amount_list = [];

    // Assuming that the server sends an array of objects with properties DealYMD and 거래금액
    for (var idx = 0; idx < rows.length; idx++) {
        _date_list.push(rows[idx].DealYMD);
        _amount_list.push(rows[idx].거래금액);
    }

    var graph = {
        x: _date_list,
        y: _amount_list,
        name: "거래금액", // You can customize the legend name
        mode: 'lines',
        marker: {
            size: 10,
            line: { width: 0.5 },
            opacity: 0.8
        }
    };

    var opt = {
        title: '두산아파트 가격 변이',
        titlefont: {
            family: 'Courier New, monospace',
            size: 24,
            color: '#7f7f7f'
        },
        xaxis: {
            title: 'DAILY',
            titlefont: {
                family: 'Arial, sans-serif',
                size: 15,
                color: 'lightgrey'
            },
            showticklabels: true,
            tickfont: {
                family: 'Old Standard TT, serif',
                size: 13,
                color: 'black'
            },
            showgrid: true,
            zeroline: true,
            showline: true,
            mirror: 'ticks',
            gridcolor: '#bdbdbd',
            gridwidth: 2,
            zerolinecolor: '#969696',
            zerolinewidth: 4,
            linecolor: '#636363',
            linewidth: 6
        },
        margin: {
            l: 50,
            r: 50,
            b: 150,
            t: 150,
            pad: 4
        },
    };

    drawingGraph([graph], opt);
}
