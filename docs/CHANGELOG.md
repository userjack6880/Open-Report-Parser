# Changelog

## 0-α5
- Fixed an issue with undefined variables discovered with Issue #12.
- Fixed an issue with mimecast improperly setting mime type outlined in Issue #12.

## 0-α4
- Fixed Issue #8 again - some errornous code was introduced at some point that skips DB checks if not using Oauth2.
- Updated documentation to fix minimum requirements regarding MariaDB vs MySQL (initially discovered with Open Report Analyzer).

## 0-α3
- Postgres fixes (and validation). Fixes Issue #8.
- Initial Oauth2 Support code (untested).

## 0-α2
- Fixed errors in previous release incorporating postgres support related to table creation.
- Added MTA-TLS report support.
- More useful debug output.
- Code consolodation (eg, subroutine repetative code).

## 0-α1
- Fork renamed
- Incorporate changes made to original repository after fork ([commit 51ba1de](https://github.com/userjack6880/Open-Report-Parser/commit/51ba1de8521559647ebe4b8a1db291c26b572de4))