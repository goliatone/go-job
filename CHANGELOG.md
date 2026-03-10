# Changelog

# [0.19.0](https://github.com/goliatone/go-job/compare/v0.18.0...v0.19.0) - (2026-03-10)

## <!-- 13 -->📦 Bumps

- Bump version: v0.19.0 ([a68a115](https://github.com/goliatone/go-job/commit/a68a115161ceb854e037bf37185464676817b978))  - (goliatone)

## <!-- 2 -->🚜 Refactor

- Clean code ([e6f9410](https://github.com/goliatone/go-job/commit/e6f94109ca041b7af0cf920a5bb8c4dca2d87d70))  - (goliatone)
- Centralize helpers in package ([5919d68](https://github.com/goliatone/go-job/commit/5919d685f0699a999bc392553ed88e8b2886191b))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.18.0 ([55e4a1a](https://github.com/goliatone/go-job/commit/55e4a1a1bac00de32922dde88cc914fca3a8b6ef))  - (goliatone)

# [0.18.0](https://github.com/goliatone/go-job/compare/v0.17.0...v0.18.0) - (2026-03-10)

## <!-- 1 -->🐛 Bug Fixes

- Dequeue redis strategy ([15a3864](https://github.com/goliatone/go-job/commit/15a38644ee5b03a8d47fe59afe7609bb322f7bd9))  - (goliatone)
- Redis keys include lease seq and prefix ([825de59](https://github.com/goliatone/go-job/commit/825de599d2de03357c41c2565c897fb0f517ebc9))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.18.0 ([5f7900f](https://github.com/goliatone/go-job/commit/5f7900f64627bfe7a8c89d0ef028917e6ef6fd00))  - (goliatone)

## <!-- 16 -->➕ Add

- Jitter to worker retry ([9ffbb02](https://github.com/goliatone/go-job/commit/9ffbb0201432bf4a767e6acc494360d3689763d8))  - (goliatone)
- Better error handling ([63885dd](https://github.com/goliatone/go-job/commit/63885dd98eeb325930f05a913c7eed1375b74ebf))  - (goliatone)
- Cancelation postgres validation ([6e49f83](https://github.com/goliatone/go-job/commit/6e49f8397d9895a5d568516ea0dd185d62b320c4))  - (goliatone)
- Create index in cancelation table ([98113e3](https://github.com/goliatone/go-job/commit/98113e39c02c0572cc20f6d485d544a27ebc3aca))  - (goliatone)
- Eval to redis client ([40a078b](https://github.com/goliatone/go-job/commit/40a078b53d80f8f839d2087cd93f6ce6ddaaa282))  - (goliatone)
- Validate storage identifiers ([13c17c5](https://github.com/goliatone/go-job/commit/13c17c576f3347e226430b3500364fde191dacd8))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.17.0 ([5daf5f6](https://github.com/goliatone/go-job/commit/5daf5f67ca1133ecc135cd7fdb90ef1d583fbe1c))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update tests ([f07cc38](https://github.com/goliatone/go-job/commit/f07cc38cf832a1703725eb20fcc089178da217e6))  - (goliatone)

# [0.17.0](https://github.com/goliatone/go-job/compare/v0.16.0...v0.17.0) - (2026-03-10)

## <!-- 1 -->🐛 Bug Fixes

- Centralize logic and clean up interfaces ([ab85bd8](https://github.com/goliatone/go-job/commit/ab85bd89298bdfb9f84f9ebd5efebf8145765cbb))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.17.0 ([993b76c](https://github.com/goliatone/go-job/commit/993b76c1b55c0b840ff713c0d294a2a87b697b54))  - (goliatone)

## <!-- 16 -->➕ Add

- Nack in cancel options ([f26880b](https://github.com/goliatone/go-job/commit/f26880b68fa7636e92631a26cb0e7876175aeaac))  - (goliatone)
- Queue option validation for nack ([991eb1b](https://github.com/goliatone/go-job/commit/991eb1bf65008d72cdc35b6667f31607e44fa978))  - (goliatone)
- Nack options ([6ecb8da](https://github.com/goliatone/go-job/commit/6ecb8dab85a26c64adaea636c36019cf878db9e1))  - (goliatone)
- Updated semantics for retry ([ca65afc](https://github.com/goliatone/go-job/commit/ca65afc2daf81b386c1afafc424e4a0ceb0680c8))  - (goliatone)
- Status and TTL for redis storage ([f4b8972](https://github.com/goliatone/go-job/commit/f4b897250683b2f9c17ed87737333ea6c9025502))  - (goliatone)
- Status key ([a56f3b1](https://github.com/goliatone/go-job/commit/a56f3b194b81b831c0a4b848f3cdd831a961d169))  - (goliatone)
- Expire command to client redis ([fafc3b9](https://github.com/goliatone/go-job/commit/fafc3b9ee5bc992dfb7668028ad484527c44d847))  - (goliatone)
- Status table to postgres ([b68b444](https://github.com/goliatone/go-job/commit/b68b44471102fffff5c531f6060909b3a78185a8))  - (goliatone)
- Postgres storage for queue revised ([105386b](https://github.com/goliatone/go-job/commit/105386b96a760bfb4302bd6a43d83364f5333ee2))  - (goliatone)
- Fix card items stack flow ([2dfc00e](https://github.com/goliatone/go-job/commit/2dfc00e5d8d62d7a700f202bc892a6b46c71771e))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.16.0 ([016d3e6](https://github.com/goliatone/go-job/commit/016d3e669347d2c47bde05785c4a009e4f22d7b7))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update docs ([6cf9bf8](https://github.com/goliatone/go-job/commit/6cf9bf8a99f0982a949cc942dc95d99b7eb26d0b))  - (goliatone)
- Update tests ([fb015ac](https://github.com/goliatone/go-job/commit/fb015ac1f80d87b2f1a71cbc8238752e159a8bb9))  - (goliatone)

# [0.16.0](https://github.com/goliatone/go-job/compare/v0.15.0...v0.16.0) - (2026-03-09)

## <!-- 13 -->📦 Bumps

- Bump version: v0.16.0 ([3ee25e3](https://github.com/goliatone/go-job/commit/3ee25e3cb25ef6947572456d36f492ae76608399))  - (goliatone)

## <!-- 16 -->➕ Add

- Queue interfaces to handle receipts, registration and metadata ([56e3da8](https://github.com/goliatone/go-job/commit/56e3da858082e2f4bdf4aeb87c72950cba90304c))  - (goliatone)
- Local worker def ([fc8a7f6](https://github.com/goliatone/go-job/commit/fc8a7f6477ae0095f8c87980d9cff8cc02837bd7))  - (goliatone)
- Queue resolver support options ([a8d639a](https://github.com/goliatone/go-job/commit/a8d639a2840f8dc383c3ba80164be5b4c2b08afb))  - (goliatone)
- Define EnqueueWithReceipt and delayed enqueue semanics ([16dbcc1](https://github.com/goliatone/go-job/commit/16dbcc187ad2fc69689c0a4a62d5adaac36d5b86))  - (goliatone)
- Queue command support for return meta and worker registration ([ce13c9f](https://github.com/goliatone/go-job/commit/ce13c9fb9f172bbbf35e2e3e2c7a585e435ba77f))  - (goliatone)
- Implement enqueue with receipt and queue status query ([f85569c](https://github.com/goliatone/go-job/commit/f85569c4743ddc688971c6e3ac962cdc1d28efc3))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.15.0 ([3dffc5e](https://github.com/goliatone/go-job/commit/3dffc5e71f4b0158867eb37c4329bb0151442fe5))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update tasks and deps ([f4d91b8](https://github.com/goliatone/go-job/commit/f4d91b8b6ef4c7db97690748b8979eceb6361d51))  - (goliatone)
- Update deps ([249d87f](https://github.com/goliatone/go-job/commit/249d87f765b1e4107e6d87eb5522feff780b40dc))  - (goliatone)
- Update tests ([65c3269](https://github.com/goliatone/go-job/commit/65c3269557db63eac7a5228cc011fd7d2eab2515))  - (goliatone)

# [0.15.0](https://github.com/goliatone/go-job/compare/v0.14.0...v0.15.0) - (2026-02-25)

## <!-- 13 -->📦 Bumps

- Bump version: v0.15.0 ([fec28aa](https://github.com/goliatone/go-job/commit/fec28aaccabce9588df12d439b9913972171286b))  - (goliatone)

## <!-- 16 -->➕ Add

- Aling task commander for idenpotency and state management ([908022c](https://github.com/goliatone/go-job/commit/908022ca6a31ff7ee634d4f65607ba94f0fae4d4))  - (goliatone)
- Terminal error ([8d6c27d](https://github.com/goliatone/go-job/commit/8d6c27d007ecfd6677d87075e6fc5f1e2f1b8454))  - (goliatone)
- Queue persistence, storage, commands, adapters, and worker ([5b0670c](https://github.com/goliatone/go-job/commit/5b0670cd2dfdeec27a4417ac0dc452b902075a6e))  - (goliatone)
- Implement enqueue tx semantics ([89a0a3e](https://github.com/goliatone/go-job/commit/89a0a3e4bc675b8b6daa7d30f1f44672471f8085))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.14.0 ([bc40d9a](https://github.com/goliatone/go-job/commit/bc40d9ae0ed99c2a9786e9a96e55ec889d901308))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update readme ([7b0d01a](https://github.com/goliatone/go-job/commit/7b0d01a3793ad18436877e3e49bf92a4e585785a))  - (goliatone)
- Update test ([40b3a1f](https://github.com/goliatone/go-job/commit/40b3a1f92d2ca3f968f81d0a501ee685d6398d1e))  - (goliatone)

# [0.14.0](https://github.com/goliatone/go-job/compare/v0.13.0...v0.14.0) - (2026-01-05)

## <!-- 1 -->🐛 Bug Fixes

- Use new decoder logic ([7c24ac7](https://github.com/goliatone/go-job/commit/7c24ac75382b282854c84db0cd9eba635239ae44))  - (goliatone)
- Queue generic command ([9a1df52](https://github.com/goliatone/go-job/commit/9a1df52c34b3d53939524dbb128ac084a95c7b69))  - (goliatone)
- Conflicting names ([2f8b4d6](https://github.com/goliatone/go-job/commit/2f8b4d6f1fa2262c7fc5b4ec2249cfe9f5dd823c))  - (goliatone)
- Use message decoder ([3789226](https://github.com/goliatone/go-job/commit/3789226cf8796f6f438f1043c5808a90a0556e8e))  - (goliatone)
- Return errors on failure ([428b483](https://github.com/goliatone/go-job/commit/428b483be97e11740dc3260e34a1074c033bca55))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.14.0 ([0fa69b7](https://github.com/goliatone/go-job/commit/0fa69b73fab04fbedc3ee34ee505e6b0b1acf2e3))  - (goliatone)

## <!-- 16 -->➕ Add

- Command resolver to match go-command interface ([80ba033](https://github.com/goliatone/go-job/commit/80ba03349db687d21cbb911ab70e48e263f4040b))  - (goliatone)
- Queue command registry adapter ([baf6065](https://github.com/goliatone/go-job/commit/baf606546d6c66a92ee7b5c0335f77987f84595f))  - (goliatone)
- Queue message codec ([0c53532](https://github.com/goliatone/go-job/commit/0c53532ef50d57f9c64416fc815c40ad18f38e1c))  - (goliatone)
- Error handler for worker ([bbf76c9](https://github.com/goliatone/go-job/commit/bbf76c9d844e7fe3f93b3bc7b78d0aa75741e0cf))  - (goliatone)
- Retry management in task commander ([bbe0ba2](https://github.com/goliatone/go-job/commit/bbe0ba28bcab9446f2beee171a7d1015fb04ad5a))  - (goliatone)
- Envelop support for jobs ([a6c8847](https://github.com/goliatone/go-job/commit/a6c8847a72ef55557db9068ef90ecc10cb6414e0))  - (goliatone)
- Envelop codec; ([b12c06e](https://github.com/goliatone/go-job/commit/b12c06e17a8e25c8018df3ef92efa9137de4e1b2))  - (goliatone)
- Queue implementation ([9af1846](https://github.com/goliatone/go-job/commit/9af18465650a7763ae80417bdddcda2b9a3023e5))  - (goliatone)

## <!-- 2 -->🚜 Refactor

- Make command register work with generic Command ([ca344fb](https://github.com/goliatone/go-job/commit/ca344fb67ee275b12a09090af459f148e367ffbe))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.13.0 ([736f801](https://github.com/goliatone/go-job/commit/736f801d5e703b4c0b427f03ca7050df7ee52b75))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update tests ([a6bf84a](https://github.com/goliatone/go-job/commit/a6bf84ae866b42506aee70775b0eb11cf196de5d))  - (goliatone)
- Update deps ([c7cca4c](https://github.com/goliatone/go-job/commit/c7cca4cf09108937a4cf589383f7e5e0aa016a3f))  - (goliatone)
- Update docs ([dcfa696](https://github.com/goliatone/go-job/commit/dcfa6966229f2ce8f77e6fda52f53cb336b51810))  - (goliatone)
- Update readme ([385179a](https://github.com/goliatone/go-job/commit/385179ac6be1d82b05c256c7561a6341df7e5be2))  - (goliatone)

# [0.13.0](https://github.com/goliatone/go-job/compare/v0.12.0...v0.13.0) - (2025-12-02)

## <!-- 1 -->🐛 Bug Fixes

- Validate messages to ensure we do not hang ([de666a5](https://github.com/goliatone/go-job/commit/de666a5b25385baac74d88fee9957280575b9abc))  - (goliatone)
- Improve error messages ([898b3b9](https://github.com/goliatone/go-job/commit/898b3b9ab5622c8ae36eda4debc62dee4370c339))  - (goliatone)
- Ensure base tasks has proper defaults ([6415971](https://github.com/goliatone/go-job/commit/64159716764303f7ec19882ffc2edc7a4f70c6c1))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.13.0 ([41b82ea](https://github.com/goliatone/go-job/commit/41b82ea2f8299dc8078a22f7f7a21b15945b6976))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.12.0 ([9007ddc](https://github.com/goliatone/go-job/commit/9007ddcf3921d0ddff14cfc9146ffa88ae03d30a))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update readme ([5bbfb23](https://github.com/goliatone/go-job/commit/5bbfb23d4ffc135c458f7468bd541b2703032a13))  - (goliatone)
- Update deps ([b7e23c4](https://github.com/goliatone/go-job/commit/b7e23c420a6a4c62a39fc486ce3041d8445cd151))  - (goliatone)

# [0.12.0](https://github.com/goliatone/go-job/compare/v0.11.0...v0.12.0) - (2025-12-02)

## <!-- 1 -->🐛 Bug Fixes

- Upgrade go-command version ([d65b023](https://github.com/goliatone/go-job/commit/d65b0235ebea46d9dc06cd1bb41c6cd32fc48d7b))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.12.0 ([5fee96d](https://github.com/goliatone/go-job/commit/5fee96d2a2bf6267c4b38b919d5328733e0a8644))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.11.0 ([178f050](https://github.com/goliatone/go-job/commit/178f0506bb389275ea005d443d2c6b5366062bb4))  - (goliatone)

# [0.11.0](https://github.com/goliatone/go-job/compare/v0.10.0...v0.11.0) - (2025-11-25)

## <!-- 13 -->📦 Bumps

- Bump version: v0.11.0 ([f35694e](https://github.com/goliatone/go-job/commit/f35694e36c0e12538a21a54574ee56040c7431f9))  - (goliatone)

## <!-- 16 -->➕ Add

- Authenticator ([aae5389](https://github.com/goliatone/go-job/commit/aae53899f8868dd7520489f1186f47ff371d79e2))  - (goliatone)
- Cron manager ([a3b3e97](https://github.com/goliatone/go-job/commit/a3b3e97ea9abdbc9317379c2e5036efd5502a909))  - (goliatone)
- Quotas manager ([c8706a0](https://github.com/goliatone/go-job/commit/c8706a002ebcee10e229bebdbb20547102f1fd43))  - (goliatone)
- Concurrency manager ([200444d](https://github.com/goliatone/go-job/commit/200444d547ab2cf1fc04ce4be887d31d078ccece))  - (goliatone)
- Result for tasks ([cb43bbd](https://github.com/goliatone/go-job/commit/cb43bbd07839fcea5a3f06501df34c760665b26f))  - (goliatone)
- Retry manager ([27a46c8](https://github.com/goliatone/go-job/commit/27a46c8ba9953bd59db252ee0884490c66261a33))  - (goliatone)
- Schedule sync command ([7567a4b](https://github.com/goliatone/go-job/commit/7567a4b0acc71a4710330ce5cce59a5a5d551230))  - (goliatone)
- Test hooks ([5670714](https://github.com/goliatone/go-job/commit/5670714ca9e03d2640b9c0ac9e248594589865f3))  - (goliatone)
- Quotas to commands ([b754aeb](https://github.com/goliatone/go-job/commit/b754aebd3eac3a4ac5e86bea6aa76fd5de4ed75c))  - (goliatone)
- Runner set IDs for result ([43683b8](https://github.com/goliatone/go-job/commit/43683b8a6dfd216cbc4c7e93c0914a38a47bd63d))  - (goliatone)
- Set/get registry ([ca537c7](https://github.com/goliatone/go-job/commit/ca537c7e5f60a5ed9a7df7a1b91fccac32dfa129))  - (goliatone)
- Job definition use tags for json/yaml ([b523924](https://github.com/goliatone/go-job/commit/b5239246b80cbd1f52a19c56bb1b83b69aaf03b6))  - (goliatone)
- Merge config ([34c9cf5](https://github.com/goliatone/go-job/commit/34c9cf5e3ef3e7b95c5b9ee47475e3bdad02da7a))  - (goliatone)
- Task  commander refactoring ([3565de2](https://github.com/goliatone/go-job/commit/3565de2b1e41df6387aaccd8f413aff963e7a8e2))  - (goliatone)
- Task commander refactoring ([4f1f1ed](https://github.com/goliatone/go-job/commit/4f1f1edc5c426113f38cbbdc2df81428719b7545))  - (goliatone)
- Idempotency key ([151fbfb](https://github.com/goliatone/go-job/commit/151fbfb6f9ccd997a6f84fec7f02433c03e82b09))  - (goliatone)
- Payload envelope ([4bf7d72](https://github.com/goliatone/go-job/commit/4bf7d72ed36c04474d643fdc9aa147c26098451e))  - (goliatone)
- Idenpotency trackers ([9b0147d](https://github.com/goliatone/go-job/commit/9b0147d72954f894952dd8595acbde7645021f6d))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.10.0 ([2ed8764](https://github.com/goliatone/go-job/commit/2ed8764d618ad5649d9338ebe2bb12e157f37246))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Udpate readme ([62c044e](https://github.com/goliatone/go-job/commit/62c044e3dd9fc8f2bb71daf5d6417f9a66bc775f))  - (goliatone)
- Update deps ([1caa023](https://github.com/goliatone/go-job/commit/1caa02356c27e8cc9d9f28cccf7f57e8a7429cc6))  - (goliatone)
- Update tests ([ac53347](https://github.com/goliatone/go-job/commit/ac53347d6c45402be95763dcc0204e6c16d6d274))  - (goliatone)

# [0.10.0](https://github.com/goliatone/go-job/compare/v0.9.0...v0.10.0) - (2025-11-22)

## <!-- 13 -->📦 Bumps

- Bump version: v0.10.0 ([fa20b0a](https://github.com/goliatone/go-job/commit/fa20b0a09823bb54aab1d5d8506b6569e7fe7e3c))  - (goliatone)

## <!-- 16 -->➕ Add

- Command adapter ([b42597e](https://github.com/goliatone/go-job/commit/b42597ed3cb8767f4d25025b0e58928f36e6aad6))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.9.0 ([9f3fcbc](https://github.com/goliatone/go-job/commit/9f3fcbcc0a816dc0a7b7120a479cafb5371fba89))  - (goliatone)

# [0.9.0](https://github.com/goliatone/go-job/compare/v0.8.0...v0.9.0) - (2025-11-22)

## <!-- 1 -->🐛 Bug Fixes

- Error messsage and handler ([dc21313](https://github.com/goliatone/go-job/commit/dc21313300df348aae4c3c769937f58410090fba))  - (goliatone)
- Secure db handling ([0bb67f0](https://github.com/goliatone/go-job/commit/0bb67f082585b1716f4c08564e46bcfba58766f8))  - (goliatone)
- Parsing yaml ([24272e0](https://github.com/goliatone/go-job/commit/24272e0a0881f4b2b7a0faeec3725b2b12384dda))  - (goliatone)
- Leak ctx in fetch ([53fd34f](https://github.com/goliatone/go-job/commit/53fd34f9dfc8db88103aafd2f231f69df430a5cb))  - (goliatone)
- Context handling ([aca03ed](https://github.com/goliatone/go-job/commit/aca03ed2415046d8ad52d30f1d816ec6934e25cf))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.9.0 ([17e180a](https://github.com/goliatone/go-job/commit/17e180af8ace22a62b10ed2ac45eb94bf49b3f95))  - (goliatone)

## <!-- 16 -->➕ Add

- Persistence options ([500fe85](https://github.com/goliatone/go-job/commit/500fe85cff8ac842e1ab5c837d78ccdfd893f6e7))  - (goliatone)
- New handler options ([8f3da9e](https://github.com/goliatone/go-job/commit/8f3da9e43d70bbf734bf6aa732502e0af330c4fa))  - (goliatone)
- Base task implement commander interface ([15569c7](https://github.com/goliatone/go-job/commit/15569c71523473b94a5af44750b9bbb24292d78b))  - (goliatone)
- Handler options ([a6c1870](https://github.com/goliatone/go-job/commit/a6c1870ec410ad3208c5a5ce3a31eafbd2bee45a))  - (goliatone)
- Task commander ([9b509b8](https://github.com/goliatone/go-job/commit/9b509b817ca76d577665de39d87828c77a08ecc8))  - (goliatone)
- Config merge ([6eef942](https://github.com/goliatone/go-job/commit/6eef942e2ed645a4d738d549dd308e58a21795a7))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.8.0 ([1effba1](https://github.com/goliatone/go-job/commit/1effba1296dd752281813c3405fc7e54658d4b95))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update tests ([2767230](https://github.com/goliatone/go-job/commit/2767230b4721b82fa78c67dd30a6387b08a34bb4))  - (goliatone)
- Update example ([0d683f2](https://github.com/goliatone/go-job/commit/0d683f2b157ccd8c2cbcfca8c0d8c8ba9cda72e0))  - (goliatone)
- Update readme ([602735e](https://github.com/goliatone/go-job/commit/602735ed2fc2b2a7dfa03b2f66fe1962b35f9f2d))  - (goliatone)

# [0.8.0](https://github.com/goliatone/go-job/compare/v0.7.0...v0.8.0) - (2025-10-27)

## <!-- 1 -->🐛 Bug Fixes

- Error check ([b4ddb00](https://github.com/goliatone/go-job/commit/b4ddb005c1e61812e16eabf26f7f40e16fc45ea2))  - (goliatone)
- Close file handlers ([934326a](https://github.com/goliatone/go-job/commit/934326a9816f7010c85cd6e08f44c99826ab672c))  - (goliatone)
- Placeholder function to build sql queries ([f58c811](https://github.com/goliatone/go-job/commit/f58c81198ad760f0c75585c20d913e73fb73c19e))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.8.0 ([611106d](https://github.com/goliatone/go-job/commit/611106dd96349946e3b22b1800cbe37518b96eb9))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.7.0 ([9a29a37](https://github.com/goliatone/go-job/commit/9a29a3728ce7388d4758bd0b431c875a518545d3))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update docs ([24b7669](https://github.com/goliatone/go-job/commit/24b76691ec66009d4cd940d178033aa01d1e7c59))  - (goliatone)

# [0.7.0](https://github.com/goliatone/go-job/compare/v0.6.0...v0.7.0) - (2025-10-27)

## <!-- 13 -->📦 Bumps

- Bump version: v0.7.0 ([8f6e9b1](https://github.com/goliatone/go-job/commit/8f6e9b1de2eb70c74e1a6caf174144aa5471d9f3))  - (goliatone)

## <!-- 16 -->➕ Add

- Schedule ([f8d1fac](https://github.com/goliatone/go-job/commit/f8d1facee7c04b1b417534649631a8f777852b08))  - (goliatone)
- Logger provider ([b09cc7b](https://github.com/goliatone/go-job/commit/b09cc7bdd104a74f7e65d965b282d429461750fe))  - (goliatone)
- Better base logging ([04c04c2](https://github.com/goliatone/go-job/commit/04c04c2549c75898edcb744c1cd916892fef172d))  - (goliatone)
- Base logger refined ([b3b177c](https://github.com/goliatone/go-job/commit/b3b177c84984bd9a24857384ec2bd7454678924b))  - (goliatone)
- Better errors and logger ([178130f](https://github.com/goliatone/go-job/commit/178130f079a1bbff98d2f102ba81a7136ced878b))  - (goliatone)
- Logger provider interface ([7da7ff2](https://github.com/goliatone/go-job/commit/7da7ff2b52381e56d71cb80b5e955c7812186fce))  - (goliatone)
- Task creator emit events ([f542658](https://github.com/goliatone/go-job/commit/f5426583b86dfc20e947804c5f1c45dc0a3d0d7b))  - (goliatone)
- Task provider implementation ([46059ad](https://github.com/goliatone/go-job/commit/46059ad012f6e7467d17195d3e48f1e9eed9aa7a))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.6.0 ([a6d8501](https://github.com/goliatone/go-job/commit/a6d8501bc83792d6a54b919cc9a89e9876ee51a6))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update deps ([4e02020](https://github.com/goliatone/go-job/commit/4e02020d86f85047c7b3490651175b96b8788b4b))  - (goliatone)
- Update readme ([36a8c04](https://github.com/goliatone/go-job/commit/36a8c04a2351a3680bd44fb06db0392f87994d0a))  - (goliatone)
- Update tests ([1f4f800](https://github.com/goliatone/go-job/commit/1f4f800235efe6a1e2d7bbc50dcf12851c4cc4c3))  - (goliatone)

# [0.6.0](https://github.com/goliatone/go-job/compare/v0.5.0...v0.6.0) - (2025-10-27)

## <!-- 1 -->🐛 Bug Fixes

- Implement defaultLogger ([33531ef](https://github.com/goliatone/go-job/commit/33531ef0cd62d7c2d5cc461fd4fc5205b2c51210))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.6.0 ([fb39c16](https://github.com/goliatone/go-job/commit/fb39c16e8b4150cc07c222f1c0fa6e40f410465b))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.5.0 ([fe9b6c2](https://github.com/goliatone/go-job/commit/fe9b6c2f993d627e067f25fbbb8066806ec47767))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update readme ([9b84f1c](https://github.com/goliatone/go-job/commit/9b84f1c0f578628ea75f94b8865a8c8184b1161c))  - (goliatone)

# [0.5.0](https://github.com/goliatone/go-job/compare/v0.4.0...v0.5.0) - (2025-09-30)

## <!-- 13 -->📦 Bumps

- Bump version: v0.5.0 ([d070a90](https://github.com/goliatone/go-job/commit/d070a906f4f57fb39932bee63e63ce3225e7e635))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.4.0 ([fa70d7f](https://github.com/goliatone/go-job/commit/fa70d7f1e3d3e71c97f2ba0a890ab5ce670d9b7e))  - (goliatone)

## <!-- 30 -->📝 Other

- PR [#1](https://github.com/goliatone/go-job/pull/1): github.com/goliatone/go-job to v0.9.0 ([85ef366](https://github.com/goliatone/go-job/commit/85ef3669184cfc512c472c4a230cebb871f81f97))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- **deps:** Bump {{ .Module }} to {{ .Version }} ([748bfaf](https://github.com/goliatone/go-job/commit/748bfaf95a01aa264aab5b8d16c94bececab1bd8))  - (goliatone)

# [0.4.0](https://github.com/goliatone/go-job/compare/v0.3.0...v0.4.0) - (2025-09-30)

## <!-- 1 -->🐛 Bug Fixes

- Remove base message ([437f92b](https://github.com/goliatone/go-job/commit/437f92b139e0e10eaa9c9a500145beb4d3239290))  - (goliatone)
- Update task creator test ([32c60cc](https://github.com/goliatone/go-job/commit/32c60cc2f6836d56fb79cdd227565bba20cde422))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.4.0 ([6591fe5](https://github.com/goliatone/go-job/commit/6591fe5ca022036628d88002e919047937992aae))  - (goliatone)

## <!-- 16 -->➕ Add

- Updated definition of ScriptInfo ([c22c188](https://github.com/goliatone/go-job/commit/c22c1880542b52a450b2b2a3fce5b8e804d92ebd))  - (goliatone)
- DB source provider ([2c23ae0](https://github.com/goliatone/go-job/commit/2c23ae005bbb2e8b756e8ef0853c01d59c103b40))  - (goliatone)
- Ensure FS source provider conforms to interface ([a6a62f0](https://github.com/goliatone/go-job/commit/a6a62f01f60934d07c84537ae0ae26074e3986af))  - (goliatone)

## <!-- 2 -->🚜 Refactor

- Use errors package ([5448938](https://github.com/goliatone/go-job/commit/5448938f8f2fdb3f0045083ac27521486ade5bc6))  - (goliatone)
- Rename to source_provider_fs ([2f45996](https://github.com/goliatone/go-job/commit/2f4599612c53e19cc1545809dfef67ff8e848b0c))  - (goliatone)
- Make set fetch public ([664d02a](https://github.com/goliatone/go-job/commit/664d02a04f727fa4868b67c6440dc48aafdd0129))  - (goliatone)

## <!-- 22 -->🚧 WIP

- Refactor errors ([7a273e5](https://github.com/goliatone/go-job/commit/7a273e5c999928ca35f4dd2bef04b459b0783f99))  - (goliatone)
- Make goja fetch implementation public ([4526c60](https://github.com/goliatone/go-job/commit/4526c6002bb3ac0d2d351539fb45793ae92a0c33))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.3.0 ([bf3995a](https://github.com/goliatone/go-job/commit/bf3995add020f2bbdf764ac546f55b74cfb1c396))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update deps ([42d29c2](https://github.com/goliatone/go-job/commit/42d29c2c0b20a01b0c0bebc46a085cd5023c8494))  - (goliatone)
- Add dev:test task ([addc756](https://github.com/goliatone/go-job/commit/addc756478926b4728e8ecee1c3569a224dee3a1))  - (goliatone)
- Update docs ([0495cb6](https://github.com/goliatone/go-job/commit/0495cb634ed973e2ed109dd35f222ebf19521fd3))  - (goliatone)
- Fix cliff ([ca35da9](https://github.com/goliatone/go-job/commit/ca35da92f8056911862103b12d0215f074055181))  - (goliatone)

# [0.3.0](https://github.com/goliatone/go-job/compare/v0.2.0...v0.3.0) - (2025-04-14)

## <!-- 13 -->📦 Bumps

- Bump version: v0.3.0 ([1b5c3eb](https://github.com/goliatone/go-job/commit/1b5c3eb9c5297f3d9c28dbe22506ca9875b389f4))  - (goliatone)

## <!-- 2 -->🚜 Refactor

- Use unified logger ([c9c55a8](https://github.com/goliatone/go-job/commit/c9c55a85eb02eea37ba7a995d0076bb40f225f5d))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.2.0 ([ce3af63](https://github.com/goliatone/go-job/commit/ce3af63e03609a7182bba8855bfeae0437a51147))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update taskfile ([b5510a1](https://github.com/goliatone/go-job/commit/b5510a18a0111c3566682b6844d4fb546f1df7c5))  - (goliatone)

# [0.2.0](https://github.com/goliatone/go-job/compare/v0.1.0...v0.2.0) - (2025-04-11)

## <!-- 13 -->📦 Bumps

- Bump version: v0.2.0 ([dc82174](https://github.com/goliatone/go-job/commit/dc82174bc104d0e363e87795780b7d13add1a79d))  - (goliatone)

## <!-- 16 -->➕ Add

- Option to configure SQL client ([4fbee27](https://github.com/goliatone/go-job/commit/4fbee271923bc8ecb6ab06a256aa80e55ca59ae0))  - (goliatone)
- Test meta ([2a01df8](https://github.com/goliatone/go-job/commit/2a01df83fb1c9827d2c344ecede340d98c3c0f65))  - (goliatone)
- Content processors so we can pre-process before parsing ([57cc00e](https://github.com/goliatone/go-job/commit/57cc00e55b7b55862fdd0dfdd13591597b20328b))  - (goliatone)

## <!-- 3 -->📚 Documentation

- Update changelog for v0.1.0 ([c6a3920](https://github.com/goliatone/go-job/commit/c6a39209c7b5d08fdd47eb95631141b9f3624fd0))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Add deps ([09309cf](https://github.com/goliatone/go-job/commit/09309cf059680ec1debf167d482bd74596d0d65c))  - (goliatone)

# [0.1.0](https://github.com/goliatone/go-job/tree/v0.1.0) - (2025-03-21)

## <!-- 1 -->🐛 Bug Fixes

- Make env optional ([ae051f6](https://github.com/goliatone/go-job/commit/ae051f6ab5ae0d91512370c43b44f62f4a2a4ee9))  - (goliatone)
- Refactor setup ([6f0a9cc](https://github.com/goliatone/go-job/commit/6f0a9cc9c4a4eac2558e391e5ea8c91c1a80b8e9))  - (goliatone)
- Update interfaces ([ce3a09c](https://github.com/goliatone/go-job/commit/ce3a09cd71e361e1915f4c1bc51c05a1d870a9d2))  - (goliatone)
- Clean path before searching ([4b7af33](https://github.com/goliatone/go-job/commit/4b7af3348d8d9bf33e658ebfbc73e66aa000afb9))  - (goliatone)
- Use a cron job interface for handler ([4b8fa0a](https://github.com/goliatone/go-job/commit/4b8fa0ae9eb212351bfbbef6872a641bb68af4d0))  - (goliatone)
- Initialize source provider if nil ([9bd459d](https://github.com/goliatone/go-job/commit/9bd459d131e7ac85330235f9ce173be975d13550))  - (goliatone)

## <!-- 13 -->📦 Bumps

- Bump version: v0.1.0 ([d6609f7](https://github.com/goliatone/go-job/commit/d6609f76034b198b08a7a169ade9eaddbd9ee7e0))  - (goliatone)

## <!-- 14 -->🎉 Initial Commit

- Initial commit ([a47ac67](https://github.com/goliatone/go-job/commit/a47ac67099f612d859b66e09fe06d42d04d2a8f3))  - (goliatone)

## <!-- 16 -->➕ Add

- Support JS block comments ([9e5c9fa](https://github.com/goliatone/go-job/commit/9e5c9fa020cc13b89dabe61e0a256d58fb984728))  - (goliatone)
- Update tests ([1d63115](https://github.com/goliatone/go-job/commit/1d631156b15ac7ad360b977480668b9a86e33053))  - (goliatone)
- Default values configurable ([2c56683](https://github.com/goliatone/go-job/commit/2c566839dd668f516cc3dd968435c2d1f774b62c))  - (goliatone)
- Use constructor functions ([f683f9a](https://github.com/goliatone/go-job/commit/f683f9ae2526c8136aecc5819f9e3d8f7ba1639e))  - (goliatone)
- Constructor to registry ([767b9be](https://github.com/goliatone/go-job/commit/767b9be56e8914468b1433371da438638ddfa6fe))  - (goliatone)
- Json tags to config ([0675d3a](https://github.com/goliatone/go-job/commit/0675d3aabc3e5e8c2ccd6e1d56b1639711f94acd))  - (goliatone)
- Task creator test ([d74d7b4](https://github.com/goliatone/go-job/commit/d74d7b43e97daa77fc5036cd4e721c3131a689fb))  - (goliatone)
- Registry test ([31416b6](https://github.com/goliatone/go-job/commit/31416b6e74128d0d358d826f6467dd9b08fa90bc))  - (goliatone)
- Parser test ([956c68c](https://github.com/goliatone/go-job/commit/956c68c991e69de11248b583a8051d487dba2150))  - (goliatone)
- New JS runner ([d170933](https://github.com/goliatone/go-job/commit/d170933bff261df83c87331d9715591c44d3574e))  - (goliatone)
- New options ([ea52546](https://github.com/goliatone/go-job/commit/ea5254674904c79baacdb636e25c9a88d70516e6))  - (goliatone)
- Fetch implementation ([31f9d8f](https://github.com/goliatone/go-job/commit/31f9d8f1697eae0702f9bd65138480f0ec1ee3c6))  - (goliatone)
- Node console imp for js runner ([9519aa6](https://github.com/goliatone/go-job/commit/9519aa6e72938ee054ca92b301e4f3a3f1fb1959))  - (goliatone)
- New SourceProvider and TaskCreator interfaces ([c522247](https://github.com/goliatone/go-job/commit/c522247bec4a38e18359d9bed3cca59dd3e33f41))  - (goliatone)
- Move fs out of base engine ([5501bc9](https://github.com/goliatone/go-job/commit/5501bc9bfdbc881767a033d60ac56904b754f679))  - (goliatone)
- Fs task ([1a88509](https://github.com/goliatone/go-job/commit/1a885098dfbe1f39be1bf681fecaca878f637314))  - (goliatone)
- Fs provider ([6cdf5e4](https://github.com/goliatone/go-job/commit/6cdf5e4e893d2fe94079ba7a2e859bf476e4eda7))  - (goliatone)
- Sql engine ([120bf91](https://github.com/goliatone/go-job/commit/120bf91b6a05589ab93557e309f73502d4e8213a))  - (goliatone)
- Shell engines ([a4d6aec](https://github.com/goliatone/go-job/commit/a4d6aec49f75e40684b2287876858bcb8a35e1c3))  - (goliatone)
- JS engine ([3f4fa61](https://github.com/goliatone/go-job/commit/3f4fa61028f35f21dc0138b40305d1a318bd14b4))  - (goliatone)
- Memory registry ([1ee82f5](https://github.com/goliatone/go-job/commit/1ee82f585441dbf53dc68251ae68ce54d7743170))  - (goliatone)
- Meta parser to use yml ([ade5dd0](https://github.com/goliatone/go-job/commit/ade5dd0446cff06dc54c46439c3cea9efe6e30c8))  - (goliatone)
- Initial interfaces ([e65463e](https://github.com/goliatone/go-job/commit/e65463e8b5a52c04b2480231023dcfce5f1505d3))  - (goliatone)
- Base task implementation ([60916cb](https://github.com/goliatone/go-job/commit/60916cb1f2f96e28fa652710c9b2d14498a21e73))  - (goliatone)
- Base engine implementation ([861b899](https://github.com/goliatone/go-job/commit/861b899731204418d5ce33f129b18f6a2a44409a))  - (goliatone)
- Options for runner ([e355c1b](https://github.com/goliatone/go-job/commit/e355c1b3cb3720dae352845165f6f763e2dcd295))  - (goliatone)
- Initial runner implementation ([86fd85a](https://github.com/goliatone/go-job/commit/86fd85a043f695181afbd1ab552cebe9dd48ad3c))  - (goliatone)

## <!-- 17 -->➖ Remove

- Unused console ([9cc31d9](https://github.com/goliatone/go-job/commit/9cc31d9598bd5c77bb3714d9f3756c8a807bf823))  - (goliatone)

## <!-- 2 -->🚜 Refactor

- Rename constructors ([409be1d](https://github.com/goliatone/go-job/commit/409be1d2ff0d6642eb4ee06557820a2fd6c6003d))  - (goliatone)
- Move files around ([5f87072](https://github.com/goliatone/go-job/commit/5f8707220b9274fd87826475accac3d09cb71b22))  - (goliatone)
- Temp fix for non oop nature of go ([c33116c](https://github.com/goliatone/go-job/commit/c33116cc4b256372635f092e56424e82b410a71d))  - (goliatone)
- Rename task creator ([c25afe1](https://github.com/goliatone/go-job/commit/c25afe10d914db49df79459b47a9039d3d5e1f1d))  - (goliatone)
- Remove unused methods ([d130cd5](https://github.com/goliatone/go-job/commit/d130cd51ce74e004881f4b16bfd2f63c426612d6))  - (goliatone)
- Fs out of runner ([6864181](https://github.com/goliatone/go-job/commit/6864181ffdaca9a2f13e0898efc0576f075093e3))  - (goliatone)

## <!-- 22 -->🚧 WIP

- Support block comments ([ec6abab](https://github.com/goliatone/go-job/commit/ec6abab21255594ced1064142cde8912b35c3c7d))  - (goliatone)
- Refactor how we handle meta config ([f11ec11](https://github.com/goliatone/go-job/commit/f11ec1198a3c256dadaf5b771c284bbc79357e9f))  - (goliatone)
- Rewrite the parser, simpler... ([26caf8c](https://github.com/goliatone/go-job/commit/26caf8c7ee15552296b086e4a632a8b24ffe3c26))  - (goliatone)
- Refactor interface ([6ab939f](https://github.com/goliatone/go-job/commit/6ab939f38a17e77215e48fc0bce3968d56d5868b))  - (goliatone)
- Refactor handlers ([c186beb](https://github.com/goliatone/go-job/commit/c186beb5cb763ee1597686aa403ff34aa41f7ed0))  - (goliatone)
- Using custom handler ([ed56db5](https://github.com/goliatone/go-job/commit/ed56db5c8a81cff5e022cd840ba89b570dc37900))  - (goliatone)
- Running require ([3bd2882](https://github.com/goliatone/go-job/commit/3bd28825e482ed0fa56db2cc816fdd7c4a670a96))  - (goliatone)

## <!-- 30 -->📝 Other

- We should document meta uses ([8d31a35](https://github.com/goliatone/go-job/commit/8d31a35e5df6791c81c5406850e3b1650e57fa73))  - (goliatone)
- Add proper log support ([4030ece](https://github.com/goliatone/go-job/commit/4030ece20425f19fd10034dcf7fa08f778bfc04c))  - (goliatone)
- Debug ([92804d6](https://github.com/goliatone/go-job/commit/92804d640ec6bed2b862c8da441e61f4c9a1f1a6))  - (goliatone)

## <!-- 7 -->⚙️ Miscellaneous Tasks

- Update README ([a770546](https://github.com/goliatone/go-job/commit/a77054677937126cdbf91b1c2a184cbb83970caf))  - (goliatone)

<!-- generated by git-cliff -->
