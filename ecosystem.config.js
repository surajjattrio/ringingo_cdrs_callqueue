module.exports = {
  apps: [{
    name: 'cdrs_posting',
    script: 'cdrs_posting.py',
    interpreter: '/mnt/myenv/bin/python3.10',
    autorestart: true,
    watch: false,
    // Safety net only, NOT the fix for the RAM leak (that's the hold_bleg_store TTL fix
    // in cdrs_posting.py itself). Set high on purpose so normal operation never comes
    // close to it - this should only ever fire if some *future*, still-unknown leak
    // shows up, not as a routine recycling mechanism.
    max_memory_restart: '500M'
  }]
};

