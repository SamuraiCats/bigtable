'use strict';

var tableTemplate = new EJS({url: '/templates/table.ejs'});
var tablesTemplate = new EJS({url: '/templates/tables.ejs'});
var notFoundTemplate = new EJS({url: '/templates/notFound.ejs'});
var queryResultsTemplate = new EJS({url: '/templates/queryResults.ejs'});

$(function () {
    loadUser();
    $(window).hashchange(onNavigate);
    onNavigate();
});

function loadUser() {
    $.get('/user', function (json) {
        console.log('/user', json);
        $('#user-name').text(json.username);
    });
}

function loadTableList() {
    $.get('/table', function (json) {
        console.log('/table', json);
        var html = tablesTemplate.render(json);
        $('#main-pane').html(html);
    });
}

function loadTable(tableName) {
    var html = tableTemplate.render({ name: tableName });
    $('#main-pane').html(html);
    $('#main-pane .query button.query').click(onQueryTable.bind(null, tableName));
}

function onQueryTable(tableName) {
    $('#main-pane .query-results').html("Loading...");
    var getData = {
        start: $('#main-pane .query .query-start').val(),
        end: $('#main-pane .query .query-end').val()
    };
    console.log('query', getData);
    $.get('/table/' + tableName, getData, function (json) {
        console.log(json);
        var html = queryResultsTemplate.render(json);
        $('#main-pane .query-results').html(html);
    });
}

function onNavigate() {
    var hash = window.location.hash;
    if (!hash) {
        loadTableList();
        return;
    }
    hash = hash.substr(1);
    displayLoading();
    if (hash.indexOf("table/") == 0) {
        loadTable(hash.substr("table/".length));
    } else {
        var html = notFoundTemplate.render({ hash: hash });
        $('#main-pane').html(html);
    }
}

function displayLoading() {
    $('#main-pane').html("Loading...");
}