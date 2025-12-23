# Tavana Release Checklist

This checklist helps ensure quality releases.

## Pre-Release

- [ ] All tests passing
- [ ] CI/CD workflows green
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in relevant files
- [ ] Breaking changes documented

## Release Process

1. **Create Release Tag**
   ```bash
   git tag -a vX.Y.Z -m "Release vX.Y.Z"
   git push origin vX.Y.Z
   ```

2. **Monitor Build**
   - Check GitHub Actions: https://github.com/Angelerator/Tavana/actions
   - Verify images pushed to Docker Hub
   - Verify Helm chart published to GHCR

3. **Test Deployment**
   ```bash
   # Run test deployment script
   SUBSCRIPTION_ID=xxx ./test-deployment.sh
   
   # Follow the output instructions
   ```

4. **Verify Release Artifacts**
   - [ ] Docker Hub: `tavana/gateway:vX.Y.Z`
   - [ ] Docker Hub: `tavana/worker:vX.Y.Z`
   - [ ] GHCR: `ghcr.io/angelerator/tavana-gateway:vX.Y.Z`
   - [ ] GHCR: `ghcr.io/angelerator/tavana-worker:vX.Y.Z`
   - [ ] GHCR: `oci://ghcr.io/angelerator/charts/tavana:X.Y.Z`
   - [ ] GitHub Release created with notes

## Post-Release

- [ ] Test deployment successful
- [ ] Run smoke tests
- [ ] Update examples to use new version
- [ ] Announce release (blog, social media, etc.)
- [ ] Monitor for issues

## Rollback Plan

If issues are discovered:

1. **Remove bad tag:**
   ```bash
   git tag -d vX.Y.Z
   git push origin :refs/tags/vX.Y.Z
   ```

2. **Delete images from Docker Hub** (if critical security issue)

3. **Create patch release** with fix

## Version Scheme

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes
- **MINOR** (0.X.0): New features, backwards compatible
- **PATCH** (0.0.X): Bug fixes, backwards compatible

## Release Notes Template

```markdown
## vX.Y.Z - YYYY-MM-DD

### Features
- New feature description (#123)

### Bug Fixes
- Bug fix description (#456)

### Breaking Changes
- Breaking change description and migration guide

### Deprecations
- Deprecated feature and alternative

### Contributors
- @contributor1
- @contributor2
```

