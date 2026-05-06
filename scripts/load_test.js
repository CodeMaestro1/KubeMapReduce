import http from 'k6/http';
import { check, sleep } from 'k6';

// k6 load testing script for KubeMapReduce Job API (Phase 3)
export const options = {
    vus: 100, // 100 virtual users
    duration: '30s', // 30 second test
};

export default function () {
    const url = 'http://localhost:8081/api/v1/jobs';

    // Minimal mock job payload
    const payload = JSON.stringify({
        spec: "sleep_job",
        reducers: 1
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            // Token would be acquired dynamically in a real load test
            'Authorization': 'Bearer test-token-mock'
        },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'is status 201 or 429': (r) => r.status === 201 || r.status === 429,
        'transaction time OK': (r) => r.timings.duration < 500,
    });

    sleep(1);
}
