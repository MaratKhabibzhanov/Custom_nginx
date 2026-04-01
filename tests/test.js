import http from 'k6/http';


// export const options = {
//   scenarios: {
//     growing_scenario: {
//       executor: "ramping-vus",
//       startVUs: 30000,
//       stages: [
//         // { duration: '5s', target: 1000 },
//         // { duration: '20s', target: 5000 },
//         { duration: '30s', target: 30000 },
//       ],
//     }
//   },
//   thresholds: {
//     http_req_failed: ['rate<0.005'],
//     http_req_duration: ['p(95)<500'],
//   },
// };


// export default function () {
//   http.post('http://localhost:8000/echo', "Hello World!!");
// }

export  let options = {
  vus: 1000,
  duration: '30s',
};

export default function () {
  http.get('http://localhost:8000');
}