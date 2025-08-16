# 🚫🚫🚫 CRITICAL WARNINGS FOR CALCULATION TRACKING SYSTEM 🚫🚫🚫

## ⚠️ WARNING TO ALL CURRENT AND FUTURE DEVELOPERS ⚠️

This system implements **REAL-TIME CALCULATION TRACKING** for scenario data changes. 

### 🔴 ABSOLUTELY PROHIBITED ACTIONS 🔴

**DO NOT UNDER ANY CIRCUMSTANCES:**

1. **Add fallback mechanisms** - No try/catch blocks that return default values
2. **Add caching systems** - No storing of button states, model lists, or change detection results  
3. **Add default button colors** - No static green/grey buttons when tracking fails
4. **Add "optimization" delays** - No batching, queuing, or deferred processing
5. **Add conditional processing** - No "if tracking works then X else Y" logic
6. **Add backwards compatibility** - No support for missing database columns

### 🟢 REQUIRED BEHAVIOR 🟢

**THE SYSTEM MUST:**

1. **Always use real-time data** - Every button state must come from fresh database queries
2. **Fail fast and loud** - If tracking fails, the entire page must fail  
3. **Update immediately** - All changes must trigger instant scenario status updates
4. **Never guess** - No assumptions about calculation status when data is missing
5. **Be 100% accurate** - Button colors must exactly reflect current database state

### 🔥 CONSEQUENCES OF VIOLATIONS 🔥

**If you add fallbacks or caching:**

- ❌ **Data Corruption**: Users will see incorrect button states
- ❌ **Missed Calculations**: Important model updates will be skipped
- ❌ **Duplicate Work**: Models will be recalculated unnecessarily  
- ❌ **User Confusion**: Green buttons when models are already calculated
- ❌ **System Unreliability**: Critical business logic will fail silently

### 📁 PROTECTED FILES

**These files have strict no-fallback policies:**

- `website/views.py` - `list_scenarios()` function
- `website/calculation_tracking.py` - All functions
- `website/signals.py` - All signal handlers  
- `templates/website/list_scenarios.html` - Button rendering logic

### 🆘 IF THE SYSTEM BREAKS

**DO NOT ADD FALLBACKS!**

Instead:
1. **Fix the root cause** - Why is the tracking system failing?
2. **Check database columns** - Are `last_calculated` and `calculation_status` present?
3. **Verify imports** - Is `calculation_tracking.py` importable?
4. **Check signals** - Are Django signals registered correctly?
5. **Ask for help** - Don't guess or add "temporary" fixes

### 💬 APPROVED RESPONSES TO COMMON REQUESTS

**"Can we add a fallback for when tracking fails?"**
- ❌ **NO** - Fix the tracking system instead

**"Can we cache button states for performance?"** 
- ❌ **NO** - Real-time accuracy is more important than performance

**"Can we show default green buttons if database is missing columns?"**
- ❌ **NO** - The database must have the required columns

**"Can we make it backwards compatible?"**
- ❌ **NO** - The system requires the new tracking columns

**"Can we add a loading state while checking for changes?"**
- ✅ **YES** - But only if it shows real-time data, not cached data

### 🎯 SYSTEM PURPOSE

This tracking system ensures that:
- Engineers see accurate calculation status
- Models are only recalculated when necessary  
- No calculations are missed due to data changes
- Button states exactly match database reality
- System behavior is 100% predictable

### 📞 EMERGENCY CONTACT

If you're tempted to add fallbacks or caching:
**STOP and ask the original system designer first!**

Remember: **Data integrity > Performance > Convenience**

---

## 🔒 ENFORCEMENT

This file serves as a **legal contract** between developers.  
By working on this system, you agree to follow these rules.

**Violation of these rules may result in:**
- Code review rejection
- System rollback to previous version
- Data integrity audits
- Performance review discussions

---

*"The best fallback is no fallback. The best cache is no cache. The best guess is no guess."*

**- Original System Designer**
