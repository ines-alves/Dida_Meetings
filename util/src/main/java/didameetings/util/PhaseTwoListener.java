package didameetings.util;

public interface PhaseTwoListener {
    void onPhaseTwoAborted(int maxballot, int instance);

    void onPhaseTwoFinish();
}
