/* Prevents copying a particular object. */
struct NoCopy {
    NoCopy() = default;
    ~NoCopy() = default;

    NoCopy(const NoCopy&) = delete;
    NoCopy& operator=(const NoCopy&) = delete;

    NoCopy(NoCopy&&) = default;
    NoCopy& operator=(NoCopy&&) = default;
};