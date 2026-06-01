-- ansible Coral source example SQL

-- These examples are organized from basic to advanced.
-- They are intentionally focused on the tables exposed by the ansible source:
------------------------------------------------------------------------------

--   ansible.hosts
--   ansible.services
--   ansible.packages
--   ansible.mounts
--   ansible.interfaces
--   ansible.security
--   ansible.roles
------------------

-- The examples avoid vendor-specific assumptions and focus on useful
-- infrastructure inventory, service-state, package, disk, network,
-- security-posture, and role-drift queries.

-- ============================================================
-- BASIC QUERIES
-- ============================================================

-- 1. List all hosts with OS, service manager, and package manager.
-- Useful first query to understand the fleet shape.
SELECT
hostname,
distribution,
distribution_version,
os_family,
service_mgr,
pkg_mgr
FROM ansible.hosts
ORDER BY distribution, hostname;

-- 2. Show the command family each host probably needs.
-- This helps avoid suggesting systemctl on Alpine/OpenRC or apt on RHEL.
SELECT
hostname,
distribution,
service_mgr,
pkg_mgr,
CASE
WHEN service_mgr = 'systemd' THEN 'systemctl status <service>'
WHEN service_mgr = 'openrc' THEN 'rc-service <service> status'
WHEN service_mgr = 'sysv' THEN 'service <service> status'
WHEN service_mgr = 'upstart' THEN 'initctl status <service>'
ELSE 'check host-specific service manager'
END AS service_check_command,
CASE
WHEN pkg_mgr IN ('dnf', 'yum') THEN pkg_mgr || ' info <package>'
WHEN pkg_mgr = 'apt' THEN 'apt show <package>'
WHEN pkg_mgr = 'apk' THEN 'apk info <package>'
WHEN pkg_mgr = 'pacman' THEN 'pacman -Qi <package>'
WHEN pkg_mgr = 'zypper' THEN 'zypper info <package>'
ELSE 'check host-specific package manager'
END AS package_check_command
FROM ansible.hosts
ORDER BY hostname;

-- 3. Find failed, stopped, or unknown services.
-- Useful during first-pass health checks.
SELECT
hostname,
name,
source,
state,
status
FROM ansible.services
WHERE LOWER(state) IN ('failed', 'stopped', 'unknown')
ORDER BY hostname, name;

-- 4. Show selected package versions across hosts.
-- Useful for quick package inventory checks.
SELECT
hostname,
source,
name,
version,
release,
arch
FROM ansible.packages
WHERE LOWER(name) IN ('python3', 'openssl', 'podman', 'postgresql', 'nginx', 'rabbitmq-server')
ORDER BY name, hostname;

-- 5. Show active network interfaces.
-- Useful for quick network inventory and MTU checks.
SELECT
hostname,
interface,
ipv4_address,
mtu,
active
FROM ansible.interfaces
WHERE active = true
ORDER BY hostname, interface;

-- 6. Show coarse security posture.
-- This intentionally uses safe posture fields only.
SELECT
hostname,
selinux_status,
selinux_mode,
selinux_policy,
apparmor_status,
fips,
firewall_hint
FROM ansible.security
ORDER BY hostname;

-- ============================================================
-- MEDIUM QUERIES
-- ============================================================

-- 7. Count hosts by package manager.
-- Useful to understand how many remediation command families exist.
SELECT
pkg_mgr,
COUNT(*) AS host_count
FROM ansible.hosts
GROUP BY pkg_mgr
ORDER BY host_count DESC, pkg_mgr;

-- 8. Count services by init/source and runtime state.
-- Useful to spot service-state spread across systemd/OpenRC/sysv/etc.
SELECT
source,
state,
COUNT(*) AS service_count
FROM ansible.services
GROUP BY source, state
ORDER BY source, state;

-- 9. Calculate disk usage percentage for all mounts.
-- Useful for capacity triage.
SELECT
hostname,
mount,
fstype,
size_available,
size_total,
ROUND((1.0 - CAST(size_available AS DOUBLE) / CAST(size_total AS DOUBLE)) * 100, 2) AS used_percent
FROM ansible.mounts
WHERE size_total > 0
ORDER BY used_percent DESC
LIMIT 20;

-- 10. Find mounts with less than 1 GiB available.
-- Useful for immediate disk pressure checks.
SELECT
hostname,
mount,
fstype,
size_available,
size_total
FROM ansible.mounts
WHERE size_available < 1073741824
ORDER BY size_available ASC;

-- 11. Join host details with unhealthy services.
-- Useful because service state alone is less helpful without distro/service manager.
SELECT
h.hostname,
h.distribution,
h.service_mgr,
h.pkg_mgr,
s.name AS service_name,
s.source AS service_source,
s.state AS service_state,
s.status AS service_status
FROM ansible.hosts h
JOIN ansible.services s
ON h.hostname = s.hostname
WHERE LOWER(s.state) IN ('failed', 'stopped', 'unknown')
ORDER BY h.hostname, s.name;

-- 12. Find hosts missing a selected package.
-- Change 'podman' to another package name when needed.
SELECT
h.hostname,
h.distribution,
h.pkg_mgr
FROM ansible.hosts h
LEFT JOIN ansible.packages p
ON h.hostname = p.hostname
AND LOWER(p.name) = 'podman'
WHERE p.name IS NULL
ORDER BY h.hostname;

-- 13. Find hosts with SELinux enforcing.
-- Useful before debugging permission-related failures.
SELECT
h.hostname,
h.distribution,
h.service_mgr,
s.selinux_status,
s.selinux_mode,
s.selinux_policy
FROM ansible.hosts h
JOIN ansible.security s
ON h.hostname = s.hostname
WHERE LOWER(s.selinux_mode) = 'enforcing'
ORDER BY h.hostname;

-- 14. Show expected services from roles with observed service state.
-- Useful for checking whether a role's expected service exists.
SELECT
r.hostname,
r.environment,
r.role,
r.expected_service,
COALESCE(s.state, 'missing') AS observed_state,
COALESCE(s.status, 'missing') AS observed_status
FROM ansible.roles r
LEFT JOIN ansible.services s
ON s.hostname = r.hostname
AND s.name = r.expected_service
WHERE r.expected_service IS NOT NULL
ORDER BY r.hostname, r.role;

-- 15. Detect role drift: expected service missing or unhealthy.
-- This is a stronger version of the previous query.
SELECT
r.hostname,
r.environment,
r.role,
r.expected_service,
COALESCE(s.state, 'missing') AS observed_state,
COALESCE(s.status, 'missing') AS observed_status
FROM ansible.roles r
LEFT JOIN ansible.services s
ON s.hostname = r.hostname
AND s.name = r.expected_service
WHERE r.expected_service IS NOT NULL
AND (s.name IS NULL OR LOWER(s.state) NOT IN ('running', 'started'))
ORDER BY r.hostname, r.role;

-- 16. Find packages installed on only one host.
-- Useful for spotting one-off package drift in small fleets.
SELECT
name,
COUNT(DISTINCT hostname) AS host_count
FROM ansible.packages
GROUP BY name
HAVING COUNT(DISTINCT hostname) = 1
ORDER BY name;

-- 17. Find host/service combinations for important services.
-- Useful for focused checks across common infrastructure services.
SELECT
h.hostname,
h.distribution,
h.service_mgr,
h.pkg_mgr,
s.name AS service_name,
COALESCE(s.state, 'missing') AS service_state,
COALESCE(s.status, 'missing') AS service_status
FROM ansible.hosts h
LEFT JOIN ansible.services s
ON s.hostname = h.hostname
AND LOWER(s.name) IN (
'nginx',
'nginx.service',
'postgresql',
'postgresql.service',
'rabbitmq-server',
'rabbitmq-server.service',
'datadog-agent',
'datadog-agent.service',
'ssh',
'ssh.service',
'sshd.service',
'sshd'
)
ORDER BY h.hostname, service_name;

-- ============================================================
-- ADVANCED QUERIES
-- ============================================================

-- 18. Host health summary using multiple evidence tables.
-- This creates a compact per-host operational summary:
--   unhealthy service count
--   low disk mount count
--   SELinux/AppArmor/FIPS posture
WITH service_health AS (
SELECT
hostname,
COUNT(*) AS unhealthy_services
FROM ansible.services
WHERE LOWER(state) IN ('failed', 'stopped', 'unknown')
GROUP BY hostname
),
disk_health AS (
SELECT
hostname,
COUNT(*) AS low_space_mounts
FROM ansible.mounts
WHERE size_available < 1073741824
GROUP BY hostname
)
SELECT
h.hostname,
h.distribution,
h.service_mgr,
h.pkg_mgr,
COALESCE(sh.unhealthy_services, 0) AS unhealthy_services,
COALESCE(dh.low_space_mounts, 0) AS low_space_mounts,
sec.selinux_mode,
sec.apparmor_status,
sec.fips
FROM ansible.hosts h
LEFT JOIN service_health sh
ON sh.hostname = h.hostname
LEFT JOIN disk_health dh
ON dh.hostname = h.hostname
LEFT JOIN ansible.security sec
ON sec.hostname = h.hostname
ORDER BY unhealthy_services DESC, low_space_mounts DESC, h.hostname;

-- 19. Rank the fullest mount per host using a window function.
-- Useful when each host has many mounts and you only want the riskiest one.
WITH mount_usage AS (
SELECT
hostname,
mount,
fstype,
size_available,
size_total,
ROUND((1.0 - CAST(size_available AS DOUBLE) / CAST(size_total AS DOUBLE)) * 100, 2) AS used_percent
FROM ansible.mounts
WHERE size_total > 0
),
ranked_mounts AS (
SELECT
hostname,
mount,
fstype,
size_available,
size_total,
used_percent,
ROW_NUMBER() OVER (
PARTITION BY hostname
ORDER BY used_percent DESC
) AS mount_rank
FROM mount_usage
)
SELECT
hostname,
mount,
fstype,
size_available,
size_total,
used_percent
FROM ranked_mounts
WHERE mount_rank = 1
ORDER BY used_percent DESC, hostname;

-- 20. Expected-service command recommendation.
-- Combines role intent, observed service state, distro, and service manager.
WITH expected_services AS (
SELECT
r.hostname,
r.role,
r.expected_service,
COALESCE(s.state, 'missing') AS observed_state,
COALESCE(s.status, 'missing') AS observed_status
FROM ansible.roles r
LEFT JOIN ansible.services s
ON s.hostname = r.hostname
AND s.name = r.expected_service
WHERE r.expected_service IS NOT NULL
)
SELECT
h.hostname,
h.distribution,
h.service_mgr,
h.pkg_mgr,
e.role,
e.expected_service,
e.observed_state,
e.observed_status,
CASE
WHEN h.service_mgr = 'systemd' THEN 'systemctl status ' || e.expected_service
WHEN h.service_mgr = 'openrc' THEN 'rc-service ' || e.expected_service || ' status'
WHEN h.service_mgr = 'sysv' THEN 'service ' || e.expected_service || ' status'
WHEN h.service_mgr = 'upstart' THEN 'initctl status ' || e.expected_service
ELSE 'check host-specific service manager for ' || e.expected_service
END AS suggested_status_command
FROM expected_services e
JOIN ansible.hosts h
ON h.hostname = e.hostname
WHERE LOWER(e.observed_state) NOT IN ('running', 'started')
ORDER BY h.hostname, e.expected_service;

-- ============================================================
-- CORAL CATALOG INTROSPECTION
-- ============================================================

-- 21. List tables exposed by the ansible source.
SELECT
schema_name,
table_name
FROM coral.tables
WHERE schema_name = 'ansible'
ORDER BY table_name;

-- 22. List columns exposed by the ansible source.
SELECT
table_name,
column_name,
data_type,
is_nullable
FROM coral.columns
WHERE schema_name = 'ansible'
ORDER BY table_name, ordinal_position;
