// config
// retry: 3
// timeout: 300s
console.log('running inside the JS runtime');
(async () => {
    console.log('pre fetch implementation...');

    // const out = await fetch(this.actionURL, {data});
    const out = await fetch("https://jsonplaceholder.typicode.com/todos/1");

    if (out.ok) {
        const json = await out.json();
        console.log('json', JSON.stringify(json));
    } else {
        console.log('error', out.status, out.statusText, out.url, out.headers);
    }
    console.log('===== END JS')
})();
