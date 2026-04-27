/*
 * Minimal OBMM ownership API for static linking into Redis.
 * Extracted from libobmm to avoid runtime dlopen/dlsym dependency.
 */

#ifndef OBMM_OWNERSHIP_H
#define OBMM_OWNERSHIP_H

/*
 * Set the ownership (reader, writer, none) of a range of OBMM virtual address space.
 * @fd: The file descriptor of an OBMM memory device.
 * @start: The start virtual address.
 * @end: The end virtual address.
 * @prot: The ownership expressed as memory protection bits (PROT_NONE, PROT_READ, PROT_WRITE).
 *        NOTE: PROT_WRITE implies PROT_READ.
 * Returns 0 on success, -1 on failure (errno set).
 */
int obmm_set_ownership(int fd, void *start, void *end, int prot);

#endif /* OBMM_OWNERSHIP_H */
