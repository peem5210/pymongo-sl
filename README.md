# pymongo-sl
pymongo-sl = pymongo SnapLogic Extended

An extended version of pymongo to have caching logic before delegating the actual query to the native pymongo library

### TODOs
- [ ] Create JIRA or something similar
- [ ] Add full-fledged unit tests for all functions
- [ ] CI/CD 

## For Contributors

### Installation for development
Use `pip install -e ./` to install the development package


### Running an End-to-End Test 
Prerequisites
1. First install the library 
2. Run a local MongoDB with no password, security, settings whatsoever using this [./docker-compose.yaml](https://github.com/peem5210/pymongo-sl/blob/master/docker-compose.yaml)
3. Populate the MongoDB data with this command `python3 rcache_profiling.py --populate`

Run end-to-end testing with `python e2e.py`



