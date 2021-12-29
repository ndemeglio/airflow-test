function convert_time(ts, convert_tz=true) {
// Function to transform EST timestamp in ms to ISO-8601 + TZid
    var date_str = new Date(ts).toISOString()
    if (convert_tz) {
        date_str = date_str.slice(0, -1) + ' America/New_York';
    }
    return date_str
}

function main() {
    return People().filter(
        function(user) {
            return {{ execution_date | start }} <= user.time
                    && user.time < {{ execution_date | end }}
        }
    ).map(function(u) {
        const distinct_id = u.distinct_id;
        const last_seen = convert_time(u.last_seen);
        const properties = JSON.stringify(u.properties);
        const labels = u.labels;
        const time = convert_time(u.time);

        return {
            'distinct_id': distinct_id,
            'last_seen': last_seen,
            'properties': properties,
            'time': time,
            'labels': labels
        };
    });
}
