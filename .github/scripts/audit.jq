# Validate first: missing data must never become an empty list of findings.
def require($condition; $message):
    if $condition then . else error($message) end;
def text: type == "string" and test("\\S");
def token: type == "string" and test("^[^[:space:]]+$");
def strings: type == "array" and all(.[]; token);
def date:
    if type != "string" or (test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$") | not) then false
    else
        split("-") | map(tonumber) | .[0] as $year | .[1] as $month | .[2] as $day
        | ($year % 4 == 0 and ($year % 100 != 0 or $year % 400 == 0)) as $leap
        | [31, (if $leap then 29 else 28 end), 31,30,31,30,31,31,30,31,30,31] as $days
        | $year > 0 and $month >= 1 and $month <= 12 and $day >= 1 and $day <= $days[$month - 1]
    end;
def vulnerabilities:
    if has("vulnerabilities") then .vulnerabilities else [] end;
def groups:
    if has("groups") then .groups else [] end;

require(length == 1; "expected one OSV document") | .[0]
| require(type == "object" and (.results | type == "array"); "missing OSV results")
| require(([.results[].source.path | gsub("\\\\"; "/")] | sort) ==
    ([$root + "/Cargo.lock"] | sort); "expected the workspace lockfile")
| require(all(.results[];
    .source.type == "lockfile" and (.packages | type == "array" and length > 0)
    and all(.packages[];
        (.package.name | token) and (.package.version | token) and .package.ecosystem == "crates.io"
        and (vulnerabilities | type == "array" and all(.[];
            (.id | token) and (if has("aliases") then (.aliases | strings) else true end)))
        and (groups | type == "array" and all(.[]; (.ids | strings and length > 0)))
        and (([groups[].ids[]] | sort) == ([vulnerabilities[].id] | sort))
    )); "invalid or incomplete package findings")
| [ .results[] as $source | $source.packages[] as $package
    | $package | groups[] | .ids as $ids
    | {package: $package.package.name, version: $package.package.version,
       source: $source.source.path,
       ids: ([$package.vulnerabilities[] | select(.id as $id | $ids | index($id))
              | .id, (.aliases // [])[]] | map(ascii_upcase) | unique)}
  ] | unique_by([.package, .version, .source, .ids]) as $findings
| require(($scanner_status == 1) == ($findings | length > 0); "scanner exit disagrees with findings")
| $policy
| require(length == 1; "expected one exception document") | .[0]
| require(type == "object" and keys == ["exceptions"] and (.exceptions | type == "array");
    "expected an exceptions array")
| .exceptions
| require(all(.[];
    type == "object" and keys == ["accepted_by", "expires_on", "id", "package", "reason"]
    and (.id | token) and (.package | token) and (.reason | text) and (.accepted_by | text)
    and (.expires_on | date)); "invalid exception fields or expiration date")
| map(.id |= ascii_upcase)
| require((group_by([.package, .id]) | all(.[]; length == 1)); "duplicate exceptions")
| . as $exceptions
| [ $findings[] as $finding
    | [$exceptions[] | select(.package == $finding.package and
        (.id as $id | $finding.ids | index($id)))]
    | require(length <= 1; "multiple exceptions cover the same finding") | .[0] as $exception
    | $finding + {exception: $exception, status:
        (if $exception == null then "REFUSED"
         elif $exception.expires_on < $today then "EXPIRED"
         else "ACCEPTED" end)}
  ] as $decisions
| {findings: $decisions, stale: [$exceptions[] as $exception
    | select(any($findings[]; .package == $exception.package and
        (.ids | index($exception.id))) | not) | $exception]}
